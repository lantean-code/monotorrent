//
// UtpConnectionTests.cs
//
// Authors:
//   Alan McGovern alan.mcgovern@gmail.com
//
// Copyright (C) 2025 Alan McGovern
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
// 
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
//


using System;
﻿using System.Buffers;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using MonoTorrent.Connections.Peer;

using NUnit.Framework;

namespace MonoTorrent.Client
{
    [TestFixture]
    public class UtpConnectionTests
    {
        UtpPeerConnectionListener Listener;
        IPeerConnection Incoming;
        IPeerConnection Outgoing;

        [SetUp]
        public void Setup ()
        {
            Listener = new UtpPeerConnectionListener (new IPEndPoint (IPAddress.Loopback, 0));

            var tcs = new TaskCompletionSource<IPeerConnection> (TaskCreationOptions.RunContinuationsAsynchronously);
            Listener.ConnectionReceived += (o, e) => tcs.TrySetResult (e.Connection);
            Listener.Start ();

            Assert.IsNotNull (Listener.LocalEndPoint, "Listener did not bind to a local endpoint");
            var uri = new Uri ($"ipv4://{Listener.LocalEndPoint}");

            Outgoing = new UtpPeerConnection (uri);

            // Initiate handshake
            _ = Outgoing.ConnectAsync ();

            // Await incoming peer
            Incoming = tcs.Task.WithTimeout (2000).GetAwaiter ().GetResult ();

            Assert.AreEqual ("ipv4", Incoming.Uri.Scheme);
            Assert.AreEqual ("ipv4", Outgoing.Uri.Scheme);
        }

        [TearDown]
        public void Teardown ()
        {
            Incoming?.Dispose ();
            Outgoing?.Dispose ();
            Listener?.Stop ();
        }

        [Test]
        public void Handshake_ConnIdsArePaired ()
        {
            var outgoingDiagnostics = GetDiagnostics (Outgoing);
            var incomingDiagnostics = GetDiagnostics (Incoming);

            WaitForEstablished (Outgoing, Incoming);
            outgoingDiagnostics = GetDiagnostics (Outgoing);
            incomingDiagnostics = GetDiagnostics (Incoming);

            Assert.That (outgoingDiagnostics.LocalConnIdSend, Is.EqualTo (outgoingDiagnostics.LocalConnIdRecv + 1));
            Assert.That (incomingDiagnostics.LocalConnIdSend, Is.EqualTo (outgoingDiagnostics.LocalConnIdRecv));
            Assert.That (incomingDiagnostics.LocalConnIdRecv, Is.EqualTo (outgoingDiagnostics.LocalConnIdSend));
            Assert.That (outgoingDiagnostics.LocalConnIdRecv % 2, Is.EqualTo (0), "Initiator receiver id should be even");
            Assert.That (incomingDiagnostics.AckNr, Is.EqualTo (1), "Responder should acknowledge the initial SYN sequence");
            Assert.That (incomingDiagnostics.SeqNr, Is.Not.EqualTo (0), "Responder sequence should be initialised");
            Assert.That (outgoingDiagnostics.SeqNr, Is.GreaterThanOrEqualTo (2), "Initiator should advance sequence after SYN");
        }

        [Test]
        public async Task SendReceive_SmallPayload ()
        {
            var data = new byte[500];
            new Random ().NextBytes (data);

            using var rSend = MemoryPool.Default.Rent (data.Length, out Memory<byte> sendBuffer);
            data.AsMemory ().CopyTo (sendBuffer);

            using var rRecv = MemoryPool.Default.Rent (data.Length, out Memory<byte> recvBuffer);

            var sendTask = Outgoing.SendAsync (sendBuffer).AsTask ();
            var recvTask = Incoming.ReceiveAsync (recvBuffer).AsTask ();

            await Task.WhenAll (sendTask.WithTimeout (2000), recvTask.WithTimeout (2000));

            Assert.AreEqual (data.Length, recvTask.Result);
            Assert.IsTrue (recvBuffer.Slice (0, data.Length).Span.SequenceEqual (data));
        }

        [Test]
        public async Task StatePacketsIncrementSequenceNumbers ()
        {
            WaitForEstablished (Outgoing, Incoming);

            ushort seqBefore = GetDiagnostics (Incoming).SeqNr;

            using var rSend = MemoryPool.Default.Rent (4, out Memory<byte> sendBuffer);
            new byte[] { 1, 2, 3, 4 }.AsMemory ().CopyTo (sendBuffer);

            using var rRecv = MemoryPool.Default.Rent (4, out Memory<byte> recvBuffer);

            var sendTask = Outgoing.SendAsync (sendBuffer).AsTask ();
            var recvTask = Incoming.ReceiveAsync (recvBuffer).AsTask ();

            await Task.WhenAll (sendTask.WithTimeout (2000), recvTask.WithTimeout (2000));

            Assert.AreEqual (4, recvTask.Result);
            Assert.IsTrue (SpinWait.SpinUntil (() => GetDiagnostics (Incoming).SeqNr > seqBefore, 1000), "#1: responder did not advance sequence number for ACK");
            Assert.That (GetDiagnostics (Incoming).AckNr, Is.EqualTo (GetDiagnostics (Outgoing).SeqNr - 1), "#2: responder ack should cover the received packet");
        }

        [Test]
        public async Task CloseWrite_EOF_OnReceive ()
        {
            // Close writer side and ensure receiver gets EOF once buffer drains
            await Outgoing.CloseWriteAsync ();

            using var r = MemoryPool.Default.Rent (64, out Memory<byte> recvBuffer);
            var bytes = await Incoming.ReceiveAsync (recvBuffer).AsTask ().WithTimeout (2000);
            Assert.AreEqual (0, bytes);
        }

        [Test]
        public async Task DisposeWhileReceiving ()
        {
            using var releaser = MemoryPool.Default.Rent (100, out Memory<byte> buffer);
            var task = Incoming.ReceiveAsync (buffer).AsTask ();
            Incoming.Dispose ();

            _ = await Task.WhenAny (task).WithTimeout (1000);
            Assert.IsTrue (task.IsCompleted, "#1");
            GC.KeepAlive (task.Exception);
        }

        [Test]
        public async Task DisposeWhileSending ()
        {
            using var releaser = MemoryPool.Default.Rent (100000, out Memory<byte> buffer);
            var task = Incoming.SendAsync (buffer).AsTask ();
            Incoming.Dispose ();

            _ = await Task.WhenAny (task).WithTimeout (1000);
            Assert.IsTrue (task.IsCompleted, "#1");
            GC.KeepAlive (task.Exception);
        }

        static void WaitForEstablished (params IPeerConnection[] connections)
        {
            Assert.IsTrue (
                SpinWait.SpinUntil (() => {
                    foreach (var connection in connections) {
                        if (!GetDiagnostics (connection).IsEstablished)
                            return false;
                    }
                    return true;
                }, 2000),
                "Handshake did not complete");
        }

        static UtpConnectionDiagnostics GetDiagnostics (IPeerConnection connection)
            => ((IUtpConnectionDiagnostics) connection).GetDiagnostics ();
    }
}
