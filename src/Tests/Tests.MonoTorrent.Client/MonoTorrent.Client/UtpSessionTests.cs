//
// UtpSessionTests.cs
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
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Threading.Tasks;

using MonoTorrent;

using NUnit.Framework;

using ReusableTasks;

namespace MonoTorrent.Client
{
    [TestFixture]
    public class UtpSessionTests
    {
        static readonly Type UtpEngineType = Type.GetType ("MonoTorrent.Connections.Peer.UtpEngine, MonoTorrent.Connections", throwOnError: true)!;
        static readonly Type UtpSessionType = Type.GetType ("MonoTorrent.Connections.Peer.UtpSession, MonoTorrent.Connections", throwOnError: true)!;
        static readonly PropertyInfo StateProperty = UtpSessionType.GetProperty ("State", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)!;
        static readonly PropertyInfo SeqProperty = UtpSessionType.GetProperty ("SeqNr", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)!;
        static readonly PropertyInfo AckProperty = UtpSessionType.GetProperty ("AckNr", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)!;
        static readonly FieldInfo DelayField = UtpSessionType.GetField ("LastDelaySampleUs", BindingFlags.Instance | BindingFlags.NonPublic)!;
        static readonly MethodInfo SendAsyncMethod = UtpSessionType.GetMethod ("SendAsync", BindingFlags.Instance | BindingFlags.NonPublic, null, new[] { typeof (ReadOnlyMemory<byte>) }, null)!;

        [Test]
        public async Task TransmitSegment_UsesLastDelaySample ()
        {
            using var receiver = new UdpClient (new IPEndPoint (IPAddress.Loopback, 0));
            var engine = Activator.CreateInstance (UtpEngineType, new object[] { new IPEndPoint (IPAddress.Loopback, 0) })!;
            UtpEngineType.GetMethod ("Start", BindingFlags.Instance | BindingFlags.Public)!.Invoke (engine, null);

            try {
                var remote = (IPEndPoint) receiver.Client.LocalEndPoint!;
                var session = Activator.CreateInstance (UtpSessionType, new object[] { engine, false, remote, (ushort) 2, (ushort) 1 })!;

                SetState (session, "Established");
                SetSeqNr (session, 10);
                SetAckNr (session, 7);
                SetDelaySample (session, 42_000u);

                var payload = new byte[] { 1 };
                var task = (ReusableTask<int>) SendAsyncMethod.Invoke (session, new object[] { new ReadOnlyMemory<byte> (payload) })!;
                await task.WithTimeout ();

                var result = await receiver.ReceiveAsync ().WithTimeout ();
                Assert.That (result.Buffer.Length, Is.GreaterThanOrEqualTo (20), "#1");

                byte packetType = (byte) (result.Buffer[0] & 0x0F);
                Assert.AreEqual (0, packetType, "#2"); // DATA packet
                uint timestampDiff = ReadUInt32 (result.Buffer.AsSpan (8));
                Assert.AreEqual (42_000u, timestampDiff, "#3");

                ((IDisposable) session).Dispose ();
            } finally {
                ((IDisposable) engine).Dispose ();
            }
        }

        static uint ReadUInt32 (ReadOnlySpan<byte> span)
            => (uint) (span[0] << 24 | span[1] << 16 | span[2] << 8 | span[3]);

        static void SetState (object session, string stateName)
        {
            var enumValue = Enum.Parse (StateProperty.PropertyType, stateName);
            StateProperty.SetValue (session, enumValue);
        }

        static void SetSeqNr (object session, ushort value)
            => SeqProperty.SetValue (session, value);

        static void SetAckNr (object session, ushort value)
            => AckProperty.SetValue (session, value);

        static void SetDelaySample (object session, uint value)
            => DelayField.SetValue (session, value);
    }
}
