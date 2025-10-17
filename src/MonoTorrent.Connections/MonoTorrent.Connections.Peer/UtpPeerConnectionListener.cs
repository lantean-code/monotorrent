//
// UtpPeerConnectionListener.cs
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
using System.Threading;

namespace MonoTorrent.Connections.Peer
{
    public sealed class UtpPeerConnectionListener : SocketListener, IPeerConnectionListener, IUtpSocketListener
    {
        public event EventHandler<PeerConnectionEventArgs>? ConnectionReceived;

        UtpEngine? Engine { get; set; }

        public UtpPeerConnectionListener (IPEndPoint endPoint)
            : base (endPoint)
        {
        }

        protected override void Start (CancellationToken token)
        {
            base.Start (token);

            var engine = Engine = new UtpEngine (PreferredLocalEndPoint);
            token.Register (() => {
                try { engine.Dispose (); } catch { }
                Engine = null;
            });

            engine.SessionEstablished += OnSessionEstablished;
            engine.Start ();

            LocalEndPoint = engine.LocalEndPoint;
        }

        public IPEndPoint? UtpLocalEndPoint => Engine?.LocalEndPoint;

        void OnSessionEstablished (UtpSession session)
        {
            try {
                var connection = UtpPeerConnection.FromSession (session);
                ConnectionReceived?.Invoke (this, new PeerConnectionEventArgs (connection, null));
            } catch {
            }
        }
    }
}
