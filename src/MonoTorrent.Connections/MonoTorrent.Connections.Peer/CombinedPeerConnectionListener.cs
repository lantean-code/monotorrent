//
// CombinedPeerConnectionListener.cs
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

using MonoTorrent.Connections;

namespace MonoTorrent.Connections.Peer
{
    /// <summary>
    /// Wraps TCP and uTP listeners and re-emits ConnectionReceived from both.
    /// </summary>
    public sealed class CombinedPeerConnectionListener : Listener, IPeerConnectionListener, IUtpSocketListener
    {
        public event EventHandler<PeerConnectionEventArgs>? ConnectionReceived;

        public IPEndPoint? LocalEndPoint { get; private set; }

        // Expose the UDP listener's bound endpoint so callers can map the correct UDP port
        public IPEndPoint? UtpLocalEndPoint { get; private set; }

        public IPEndPoint PreferredLocalEndPoint { get; }

        IPeerConnectionListener TcpListener { get; }
        IPeerConnectionListener UtpListener { get; }

        public CombinedPeerConnectionListener (IPEndPoint endPoint)
        {
            PreferredLocalEndPoint = endPoint ?? throw new ArgumentNullException (nameof (endPoint));

            TcpListener = new PeerConnectionListener (endPoint);
            UtpListener = new UtpPeerConnectionListener (endPoint);

            TcpListener.ConnectionReceived += OnConnectionReceived;
            UtpListener.ConnectionReceived += OnConnectionReceived;
        }

        protected override void Start (CancellationToken token)
        {
            // Ensure we stop child listeners when this is cancelled
            token.Register (() => {
                try { TcpListener.Stop (); } catch { }
                try { UtpListener.Stop (); } catch { }
                LocalEndPoint = null;
            });

            TcpListener.Start ();
            UtpListener.Start ();

            // Prefer the TCP endpoint for reporting purposes
            LocalEndPoint = TcpListener.LocalEndPoint;
            UtpLocalEndPoint = UtpListener.LocalEndPoint;
        }

        void OnConnectionReceived (object? sender, PeerConnectionEventArgs e)
        {
            ConnectionReceived?.Invoke (this, e);
        }
    }
}
