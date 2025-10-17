//
// UtpPeerConnection.cs
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
using System.Threading.Tasks;

using ReusableTasks;

namespace MonoTorrent.Connections.Peer
{
    public sealed class UtpPeerConnection : IPeerConnection, IUtpConnectionDiagnostics
    {
        public ReadOnlyMemory<byte> AddressBytes { get; }

        public bool CanReconnect => !IsIncoming;

        public bool Disposed { get; private set; }

        public IPEndPoint EndPoint { get; }

        public bool IsIncoming { get; }

        public Uri Uri { get; }

        readonly UtpEngine Engine;
        readonly UtpSession Session;

        UtpPeerConnection (UtpSession session, bool isIncoming)
        {
            Session = session ?? throw new ArgumentNullException (nameof (session));
            IsIncoming = isIncoming;
            Engine = session.Engine;

            EndPoint = session.RemoteEndPoint;
            AddressBytes = EndPoint.Address.GetAddressBytes ();
            Uri = EndPoint.AddressFamily switch {
                System.Net.Sockets.AddressFamily.InterNetwork => new Uri ($"ipv4://{EndPoint}"),
                System.Net.Sockets.AddressFamily.InterNetworkV6 => new Uri ($"ipv6://{EndPoint}"),
                _ => new Uri ($"ipv4://{EndPoint}")
            };
        }

        internal UtpPeerConnection (Uri uri, UtpEngine engine)
        {
            if (uri == null) throw new ArgumentNullException (nameof (uri));
            if (engine == null) throw new ArgumentNullException (nameof (engine));

            Engine = engine;
            Uri = uri;
            IsIncoming = false;

            EndPoint = new IPEndPoint (IPAddress.Parse (uri.Host), uri.Port);
            AddressBytes = EndPoint.Address.GetAddressBytes ();

            Session = Engine.CreateOutgoingSession (EndPoint);
        }

        internal static UtpPeerConnection FromSession (UtpSession session)
            => new UtpPeerConnection (session, true);

        public UtpPeerConnection (Uri uri)
            : this (CreateSession (uri), false)
        {
        }

        static UtpSession CreateSession (Uri uri)
        {
            if (uri == null)
                throw new ArgumentNullException (nameof (uri));

            var remote = new IPEndPoint (IPAddress.Parse (uri.Host), uri.Port);
            var local = remote.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork
                ? new IPEndPoint (IPAddress.Any, 0)
                : new IPEndPoint (IPAddress.IPv6Any, 0);

            var engine = new UtpEngine (local);
            engine.Start ();
            return engine.CreateOutgoingSession (remote);
        }

        public async ReusableTask ConnectAsync ()
        {
            if (IsIncoming)
                throw new InvalidOperationException ("This connection represents an incoming connection");
            await Session.ConnectAsync ();
        }

        public ReusableTask<int> ReceiveAsync (Memory<byte> buffer)
        {
            if (Disposed)
                throw new ObjectDisposedException (nameof (UtpPeerConnection));
            return Session.ReceiveAsync (buffer);
        }

        public ReusableTask<int> SendAsync (Memory<byte> buffer)
        {
            if (Disposed)
                throw new ObjectDisposedException (nameof (UtpPeerConnection));
            return Session.SendAsync (buffer);
        }

        public async ReusableTask CloseWriteAsync ()
        {
            Session.InitiateFin ();
        }

        public async ReusableTask CloseAsync ()
        {
            Session.InitiateFin ();
            // Await FIN handshake completion with a conservative timeout to keep this non-blocking in practice
            var finished = Session.WhenFullyClosed.AsTask ();
            var timeout = Task.Delay (300);
            await Task.WhenAny (finished, timeout);
        }

        public void Dispose ()
        {
            Disposed = true;
            try {
                if (!Session.IsClosingGracefully)
                    Session.SendReset ();
            } catch { }
            try { Session.Dispose (); } catch { }
            try { if (!IsIncoming) Engine.Dispose (); } catch { }
        }

        public UtpConnectionDiagnostics GetDiagnostics ()
        {
            var session = Session;
            return new UtpConnectionDiagnostics (
                session.State == UtpSessionState.Established,
                IsIncoming,
                session.LocalConnIdSend,
                session.LocalConnIdRecv,
                session.SeqNr,
                session.AckNr
            );
        }
    }
}
