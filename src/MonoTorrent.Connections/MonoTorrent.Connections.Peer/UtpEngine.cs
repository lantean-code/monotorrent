//
// UtpEngine.cs
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
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

using ReusableTasks;

namespace MonoTorrent.Connections.Peer
{
    /// <summary>
    /// A per-endpoint engine which handles receiving/sending uTP packets over UDP, demultiplexes
    /// packets to the appropriate <see cref="UtpSession"/>, and maintains periodic timers
    /// for retransmissions and congestion control.
    /// </summary>
    /// <remarks>
    /// This class intentionally runs off the ClientEngine.MainLoop. Callers must marshal to the
    /// ClientEngine.MainLoop when interacting with client state.
    /// </remarks>
    internal sealed class UtpEngine : IDisposable
    {
        readonly Dictionary<(IPEndPoint Remote, ushort ConnIdRecv), UtpSession> Sessions;
        readonly CancellationTokenSource Cancellation;

        public event Action<UtpSession>? SessionEstablished;

        public IPEndPoint LocalEndPoint { get; private set; }

        UdpClient? Client { get; set; }

        Task? ReceiveLoopTask { get; set; }
        Task? TimerLoopTask { get; set; }

        // Thread-safe random compatible with older frameworks
        static int Seed = Environment.TickCount;
        static readonly ThreadLocal<Random> ThreadRandom = new ThreadLocal<Random> (() => new Random (Interlocked.Increment (ref Seed)));

        public UtpEngine (IPEndPoint localEndPoint)
        {
            LocalEndPoint = localEndPoint ?? throw new ArgumentNullException (nameof (localEndPoint));
            Sessions = new Dictionary<(IPEndPoint, ushort), UtpSession> (new RemoteKeyComparer ());
            Cancellation = new CancellationTokenSource ();
        }

        public void Start ()
        {
            if (Client != null)
                return;

            Client = new UdpClient (LocalEndPoint);
            try {
                LocalEndPoint = (IPEndPoint) Client.Client.LocalEndPoint!;
            } catch {
            }
            ReceiveLoopTask = ReceiveLoopAsync (Client, Cancellation.Token);
            TimerLoopTask = TimerLoopAsync (Cancellation.Token);
        }

        public void Stop ()
        {
            Cancellation.Cancel ();
            var client = Client;
            Client = null;
            try { client?.Dispose (); } catch { }
        }

        public void Dispose ()
        {
            Stop ();
        }

        internal UtpSession CreateOutgoingSession (IPEndPoint remote)
        {
            // Per spec the initiator chooses a random receive conn_id, and transmits the SYN using that value.
            // Subsequent packets use conn_id+1. Choose even ids to match libutp's behaviour and avoid wraparound.
            ushort localRecvId = (ushort) (ThreadRandom.Value!.Next (1, ushort.MaxValue) & ~1);
            ushort localSendId = (ushort) (localRecvId + 1);

            var session = new UtpSession (this, isIncoming: false, remoteEndPoint: remote, localConnIdSend: localSendId, localConnIdRecv: localRecvId);
            lock (Sessions) {
                Sessions[(remote, localRecvId)] = session;
                Sessions[(remote, localSendId)] = session;
            }
            return session;
        }

        internal UdpClient RequireClient ()
            => Client ?? throw new InvalidOperationException ("The UtpEngine has not been started");

        internal void OnSessionEstablished (UtpSession session)
            => SessionEstablished?.Invoke (session);

        internal void RemoveSession (UtpSession session)
        {
            lock (Sessions) {
                Sessions.Remove ((session.RemoteEndPoint, session.LocalConnIdRecv));
                Sessions.Remove ((session.RemoteEndPoint, session.LocalConnIdSend));
            }
        }

        async Task ReceiveLoopAsync (UdpClient client, CancellationToken token)
        {
            while (!token.IsCancellationRequested) {
                try {
#if NET6_0
                    var result = await client.ReceiveAsync (token).ConfigureAwait (false);
#else
                    var result = await client.ReceiveAsync ().ConfigureAwait (false);
#endif
                    if (token.IsCancellationRequested)
                        break;

                    HandleIncomingPacket (result.Buffer, result.RemoteEndPoint);
                } catch (SocketException ex) {
                    if (ex.ErrorCode == 10054)
                        continue;
                } catch {
                }
            }
        }

        async Task TimerLoopAsync (CancellationToken token)
        {
            // A simple periodic timer to drive retransmits and cwnd updates.
            var delay = TimeSpan.FromMilliseconds (25);
            while (!token.IsCancellationRequested) {
                try {
                    await Task.Delay (delay, token).ConfigureAwait (false);
                } catch {
                    break;
                }

                lock (Sessions) {
                    foreach (var s in Sessions.Values)
                        s.OnTimerTick ();
                }
            }
        }

        void HandleIncomingPacket (byte[] buffer, IPEndPoint remote)
        {
            if (buffer == null || buffer.Length < 20)
                return;

            if (!UtpPacket.TryParse (buffer, out var packet))
                return;

            UtpSession? session = null;
            lock (Sessions) {
                // Demux by the remote's send connection id as present in the header
                Sessions.TryGetValue ((remote, packet.ConnIdSend), out session);
                if (session == null && packet.Type == UtpPacketType.Syn) {
                    // Create inbound session: remote's send id is our recv id; choose our send id as recv+1
                    ushort remoteSendId = packet.ConnIdSend;
                    ushort localSendId = remoteSendId;
                    ushort localRecvId = (ushort) (remoteSendId + 1);
                    session = new UtpSession (this, isIncoming: true, remoteEndPoint: remote, localConnIdSend: localSendId, localConnIdRecv: localRecvId);
                    Sessions[(remote, remoteSendId)] = session;
                    Sessions[(remote, localRecvId)] = session;
                }
            }

            if (session != null)
                session.HandleIncomingPacket (packet, buffer.AsMemory (UtpPacket.HeaderLength));
        }

        sealed class RemoteKeyComparer : IEqualityComparer<(IPEndPoint Remote, ushort ConnIdRecv)>
        {
            public bool Equals ((IPEndPoint Remote, ushort ConnIdRecv) x, (IPEndPoint Remote, ushort ConnIdRecv) y)
                => x.ConnIdRecv == y.ConnIdRecv && x.Remote.Equals (y.Remote);

            public int GetHashCode ((IPEndPoint Remote, ushort ConnIdRecv) obj)
            {
                unchecked {
                    int hash = 17;
                    hash = (hash * 31) + obj.Remote.GetHashCode ();
                    hash = (hash * 31) + obj.ConnIdRecv.GetHashCode ();
                    return hash;
                }
            }
        }
    }

    enum UtpPacketType : byte
    {
        Data = 0,
        Fin = 1,
        State = 2,
        Reset = 3,
        Syn = 4,
    }

    struct UtpPacket
    {
        public const int HeaderLength = 20;

        public byte Version;
        public UtpPacketType Type;
        public byte Extension;
        public ushort ConnIdSend;
        public uint TimestampMicroseconds;
        public uint TimestampDiffMicroseconds;
        public uint WindowSize;
        public ushort SeqNr;
        public ushort AckNr;
        public ushort ConnIdRecv; // Derived: send-1 or recv id

        public static bool TryParse (ReadOnlySpan<byte> buffer, out UtpPacket packet)
        {
            packet = default;
            if (buffer.Length < HeaderLength)
                return false;

            byte verType = buffer[0];
            packet.Type = (UtpPacketType) (verType & 0x0F);
            packet.Version = (byte) (verType >> 4);
            packet.Extension = buffer[1];
            packet.ConnIdSend = ReadUInt16 (buffer.Slice (2));
            packet.TimestampMicroseconds = ReadUInt32 (buffer.Slice (4));
            packet.TimestampDiffMicroseconds = ReadUInt32 (buffer.Slice (8));
            packet.WindowSize = ReadUInt32 (buffer.Slice (12));
            packet.SeqNr = ReadUInt16 (buffer.Slice (16));
            packet.AckNr = ReadUInt16 (buffer.Slice (18));

            // The spec defines a pair of connection ids: send and receive are consecutive.
            // We derive the expected recv id as send-1. For inbound SYN we flip when creating a session.
            unchecked { packet.ConnIdRecv = (ushort) (packet.ConnIdSend - 1); }
            return true;
        }

        public static int Write (Span<byte> buffer, UtpPacketType type, byte extension, ushort connIdSend, uint tsUs, uint tsDiffUs, uint wndSize, ushort seq, ushort ack)
        {
            if (buffer.Length < HeaderLength)
                throw new ArgumentException ("Insufficient buffer for uTP header");

            buffer[0] = (byte) ((1 << 4) | ((byte) type & 0x0F));
            buffer[1] = extension;
            WriteUInt16 (buffer.Slice (2), connIdSend);
            WriteUInt32 (buffer.Slice (4), tsUs);
            WriteUInt32 (buffer.Slice (8), tsDiffUs);
            WriteUInt32 (buffer.Slice (12), wndSize);
            WriteUInt16 (buffer.Slice (16), seq);
            WriteUInt16 (buffer.Slice (18), ack);
            return HeaderLength;
        }

        static ushort ReadUInt16 (ReadOnlySpan<byte> b)
            => (ushort) (b[0] << 8 | b[1]);
        static uint ReadUInt32 (ReadOnlySpan<byte> b)
            => (uint) (b[0] << 24 | b[1] << 16 | b[2] << 8 | b[3]);

        static void WriteUInt16 (Span<byte> b, ushort v)
        {
            b[0] = (byte) (v >> 8);
            b[1] = (byte) (v);
        }
        static void WriteUInt32 (Span<byte> b, uint v)
        {
            b[0] = (byte) (v >> 24);
            b[1] = (byte) (v >> 16);
            b[2] = (byte) (v >> 8);
            b[3] = (byte) (v);
        }
    }
}
