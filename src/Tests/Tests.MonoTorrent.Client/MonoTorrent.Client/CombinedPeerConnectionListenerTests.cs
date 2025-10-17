//
// CombinedPeerConnectionListenerTests.cs
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
using System.Threading.Tasks;

using MonoTorrent;
using MonoTorrent.Connections.Peer;

using NUnit.Framework;

namespace MonoTorrent.Client
{
    [TestFixture]
    public class CombinedPeerConnectionListenerTests
    {
        [Test]
        public async Task AcceptsTcpAndUtpConnections ()
        {
            var listener = new CombinedPeerConnectionListener (new IPEndPoint (IPAddress.Loopback, 0));
            var tcpConnectionReceived = new TaskCompletionSource<IPeerConnection> (TaskCreationOptions.RunContinuationsAsynchronously);
            var utpConnectionReceived = new TaskCompletionSource<IPeerConnection> (TaskCreationOptions.RunContinuationsAsynchronously);

            listener.ConnectionReceived += (sender, args) => {
                if (args.Connection is UtpPeerConnection)
                    utpConnectionReceived.TrySetResult (args.Connection);
                else
                    tcpConnectionReceived.TrySetResult (args.Connection);
            };

            listener.Start ();

            Assert.IsNotNull (listener.LocalEndPoint, "TCP endpoint should be initialised");
            Assert.IsNotNull (listener.UtpLocalEndPoint, "uTP endpoint should be initialised");

            using var tcpClient = new Socket (AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            tcpClient.Connect (listener.LocalEndPoint);
            var tcpServerConnection = await tcpConnectionReceived.Task.WithTimeout ();

            var utpClient = new UtpPeerConnection (new Uri ($"ipv4://{listener.UtpLocalEndPoint}"));
            await utpClient.ConnectAsync ().WithTimeout ();
            var utpServerConnection = await utpConnectionReceived.Task.WithTimeout ();

            Assert.AreEqual ("ipv4", tcpServerConnection.Uri.Scheme, "#1");
            Assert.AreEqual ("ipv4", utpServerConnection.Uri.Scheme, "#2");

            utpClient.Dispose ();
            tcpServerConnection.Dispose ();
            utpServerConnection.Dispose ();
            listener.Stop ();
        }
    }
}
