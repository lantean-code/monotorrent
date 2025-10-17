//
// UtpSession.cs
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
using System.Threading;
using System.Diagnostics;
using System.Security.Cryptography;

using ReusableTasks;

namespace MonoTorrent.Connections.Peer
{
    enum UtpSessionState
    {
        Closed,
        SynSent,
        SynReceived,
        Established,
        FinWait,
        Reset,
    }

    /// <summary>
    /// Represents a single uTP session. This exposes a stream-like interface to higher layers
    /// via the adapter, and contains the state machine, reliability and congestion control.
    /// </summary>
    internal sealed class UtpSession : IDisposable
    {
        public bool IsIncoming { get; }

        public IPEndPoint RemoteEndPoint { get; }

        public ushort LocalConnIdSend { get; }
        public ushort LocalConnIdRecv { get; }

        public ushort SeqNr { get; private set; }
        public ushort AckNr { get; private set; }

        public UtpSessionState State { get; private set; }

        internal UtpEngine Engine { get; }

        readonly ReusableTaskCompletionSource<int> ConnectTcs;
        readonly ReusableTaskCompletionSource<bool> CloseTcs = new ReusableTaskCompletionSource<bool> ();
        bool CloseSignalled;

        // Receive reassembly
        byte[]? RxBuffer;
        int RxBufferCount;
        ushort NextExpectedSeq;
        System.Collections.Generic.Dictionary<ushort, byte[]> ReorderBuffer;

        readonly ReusableTaskCompletionSource<int> DataAvailableTcs;

        // Congestion control and reliability
        const int DefaultMtu = 1200; // payload target
        const int MinRtoUs = 250_000; // 250ms
        const int MaxRtoUs = 60_000_000; // 60s
        const int TargetDelayUs = 100_000; // 100ms LEDBAT target
        const int AckDelayUs = 25_000; // delayed ACK threshold ~25ms
        const double LedbatGain = 1.0 / 300.0; // libutp-like gain
        const int KeepAliveIntervalUs = 30_000_000; // 30s
        int MssBytes = DefaultMtu;
        int CwndBytes = 3 * DefaultMtu;
        int SsthreshBytes = 64 * DefaultMtu;
        int BytesInFlight;
        uint BaseDelayUs = uint.MaxValue;
        uint SrttUs;
        uint RttvarUs = 125_000; // 125ms initial
        uint RtoUs = 1_000_000; // 1s initial

        uint LastActivityUs;
        uint LastKeepAliveSentUs;
        uint LastPacketArrivalUs;
        uint LastDelaySampleUs;
        int PendingAcks;
        uint AckDueTimeUs;
        // Fast retransmit tracking (dup-ack heuristic)
        ushort LastAckNr;
        uint LastSackMask;
        int DuplicateAckCount;

        // Sliding window for base delay (min over recent samples)
        const int BaseDelayWindowUs = 30_000_000; // 30 seconds
        const int BaseDelaySampleCount = 256;
        uint[] BaseDelayValues = new uint[BaseDelaySampleCount];
        uint[] BaseDelayTimes = new uint[BaseDelaySampleCount];
        int BaseDelayIndex;
        int BaseDelayUsed;

        // Advertised receive window (bytes we can still accept)
        int RxWindowBytes = 64 * 1024;
        int RemoteWindowBytes = int.MaxValue;

        class OutSeg
        {
            public ushort Seq;
            public byte[] Data = Array.Empty<byte>();
            public int Len;
            public bool Sent;
            public bool Acked;
            public bool FastRetxDone;
            public uint SentAtUs;
            public uint DeadlineUs; // retransmit time
            public int Retransmits;
            public bool IsFin;
        }
        System.Collections.Generic.LinkedList<OutSeg> SendQueue = new System.Collections.Generic.LinkedList<OutSeg> ();

        readonly object SendLocker = new object ();
        readonly object ReceiveLocker = new object ();

        static readonly RandomNumberGenerator Rng = RandomNumberGenerator.Create ();

        static ushort GenerateInitialSeq ()
        {
            var buffer = new byte[2];
            Rng.GetBytes (buffer);
            ushort value = (ushort) (buffer[0] << 8 | buffer[1]);
            return value == 0 ? (ushort) 1 : value;
        }

        public UtpSession (UtpEngine engine, bool isIncoming, IPEndPoint remoteEndPoint, ushort localConnIdSend, ushort localConnIdRecv)
        {
            Engine = engine ?? throw new ArgumentNullException (nameof (engine));
            IsIncoming = isIncoming;
            RemoteEndPoint = remoteEndPoint ?? throw new ArgumentNullException (nameof (remoteEndPoint));
            LocalConnIdSend = localConnIdSend;
            LocalConnIdRecv = localConnIdRecv;

            ConnectTcs = new ReusableTaskCompletionSource<int> ();
            DataAvailableTcs = new ReusableTaskCompletionSource<int> ();

            State = isIncoming ? UtpSessionState.SynReceived : UtpSessionState.Closed;
            SeqNr = isIncoming ? (ushort) 0 : (ushort) 1;
            AckNr = 0;
            NextExpectedSeq = 1;
            ReorderBuffer = new System.Collections.Generic.Dictionary<ushort, byte[]> ();
            LastActivityUs = NowMicroseconds ();
            LastPacketArrivalUs = LastActivityUs;
            LastAckNr = 0;
            LastSackMask = 0;
            DuplicateAckCount = 0;
        }

        public void Dispose ()
        {
            State = UtpSessionState.Closed;
            Engine.RemoveSession (this);
            RxBuffer = null;
            RxBufferCount = 0;
            // Wake any pending receivers so they do not block indefinitely
            try { DataAvailableTcs.SetResult (0); } catch { }
            SignalClosed (); 
        }

        public async ReusableTask ConnectAsync ()
        {
            if (IsIncoming)
                throw new InvalidOperationException ("ConnectAsync cannot be used for incoming sessions");
            if (State != UtpSessionState.Closed)
                return;

            // Send SYN
            var nowUs = NowMicroseconds ();
            var header = new byte[UtpPacket.HeaderLength];
            var written = UtpPacket.Write (header, UtpPacketType.Syn, 0, LocalConnIdRecv, nowUs, 0, 0, SeqNr, AckNr);
            await Engine.RequireClient ().SendAsync (header, written, RemoteEndPoint).ConfigureAwait (false);
            State = UtpSessionState.SynSent;
            SeqNr++;

            await ConnectTcs.Task;
        }

        public void OnTimerTick ()
        {
            if (State != UtpSessionState.Established && State != UtpSessionState.SynSent && State != UtpSessionState.SynReceived)
                return;

            var nowUs = NowMicroseconds ();

            // Retransmissions and timeouts
            bool lossDetected = false;
            for (var node = SendQueue.First; node != null; node = node.Next) {
                var seg = node.Value;
                if (!seg.Sent || seg.Acked)
                    continue;
                if ((uint) (nowUs - seg.SentAtUs) >= seg.DeadlineUs) {
                    // RTO: retransmit
                    lossDetected = true;
                    seg.Retransmits++;
                    // Exponential backoff but clamp
                    seg.DeadlineUs = (uint) Math.Min (MaxRtoUs, Math.Max (MinRtoUs, RtoUs << Math.Min (seg.Retransmits, 6)));
                    TransmitSegment (seg, nowUs);
                }
            }

            if (lossDetected) {
                // Multiplicative decrease
                CwndBytes = Math.Max (2 * MssBytes, CwndBytes / 2);
                SsthreshBytes = CwndBytes;
            }

            // Keepalive if idle
            if ((nowUs - LastActivityUs) >= KeepAliveIntervalUs && State == UtpSessionState.Established) {
                SendStateAckInternal (AckNr);
                LastKeepAliveSentUs = nowUs;
            }

            // Try to send pending segments within cwnd
            TrySendPending (nowUs);

            // Delayed ACK timer
            if (PendingAcks > 0) {
                if ((uint) (nowUs - AckDueTimeUs) < 0x8000_0000) {
                    SendStateAckInternal (AckNr);
                    PendingAcks = 0;
                }
            }
        }

        internal ReusableTask<int> ReceiveAsync (Memory<byte> buffer)
        {
            // Fast path: deliver available bytes
            if (RxBufferCount > 0) {
                int toCopy;
                lock (ReceiveLocker) {
                    toCopy = Math.Min (buffer.Length, RxBufferCount);
                    if (toCopy > 0) {
                        RxBuffer.AsSpan (0, toCopy).CopyTo (buffer.Span);
                        if (toCopy < RxBufferCount)
                            System.Buffer.BlockCopy (RxBuffer!, toCopy, RxBuffer!, 0, RxBufferCount - toCopy);
                        RxBufferCount -= toCopy;
                    }
                }
                var tcs = new ReusableTaskCompletionSource<int> ();
                tcs.SetResult (toCopy);
                return tcs.Task;
            }

            // EOF semantics: if FIN received and no buffered data, return 0
            if (FinReceived) {
                var tcs = new ReusableTaskCompletionSource<int> ();
                tcs.SetResult (0);
                return tcs.Task;
            }

            return AwaitDataAndCopyAsync (buffer);
        }

        async ReusableTask<int> AwaitDataAndCopyAsync (Memory<byte> buffer)
        {
            // Await the next data notification
            await DataAvailableTcs.Task;
            int toCopy;
            lock (ReceiveLocker) {
                toCopy = Math.Min (buffer.Length, RxBufferCount);
                if (toCopy > 0) {
                    RxBuffer.AsSpan (0, toCopy).CopyTo (buffer.Span);
                    if (toCopy < RxBufferCount)
                        System.Buffer.BlockCopy (RxBuffer!, toCopy, RxBuffer!, 0, RxBufferCount - toCopy);
                    RxBufferCount -= toCopy;
                } else if (FinReceived) {
                    // EOF: FIN received and no data available
                    return 0;
                }
            }
            return toCopy;
        }

        internal ReusableTask<int> SendAsync (ReadOnlyMemory<byte> buffer)
        {
            var nowUs = NowMicroseconds ();
            int offset = 0;
            lock (SendLocker) {
                while (offset < buffer.Length) {
                    int len = Math.Min (MssBytes, buffer.Length - offset);
                    var seg = new OutSeg {
                        Seq = SeqNr++,
                        Data = buffer.Slice (offset, len).ToArray (),
                        Len = len,
                    };
                    SendQueue.AddLast (seg);
                    offset += len;
                }
            }
            TrySendPending (nowUs);
            var tcs = new ReusableTaskCompletionSource<int> ();
            tcs.SetResult (buffer.Length);
            return tcs.Task;
        }

        internal ReusableTask<bool> WhenFullyClosed => CloseTcs.Task;

        internal void HandleIncomingPacket (UtpPacket packet, ReadOnlyMemory<byte> payload)
        {
            // Record local arrival time and last remote timestamp for timestamp_difference
            LastPacketArrivalUs = NowMicroseconds ();
            LastDelaySampleUs = unchecked (LastPacketArrivalUs - packet.TimestampMicroseconds);
            // Compute extension length so we can strip it from DATA payloads
            int extLen = GetExtensionsLength (packet.Extension, payload.Span);
            var dataPayload = extLen <= payload.Length ? payload.Slice (extLen) : ReadOnlyMemory<byte>.Empty;
            switch (packet.Type) {
                case UtpPacketType.State:
                    HandleState (packet, payload);
                    break;
                case UtpPacketType.Data:
                    HandleData (packet, dataPayload);
                    break;
                case UtpPacketType.Fin:
                    HandleFin (packet);
                    break;
                case UtpPacketType.Reset:
                    HandleReset ();
                    break;
                case UtpPacketType.Syn:
                    HandleSyn (packet);
                    break;
            }
        }

        void HandleSyn (UtpPacket packet)
        {
            // For inbound this is the first packet. Send a STATE ack to confirm and move to Established.
            if (!IsIncoming)
                return;

            AckNr = packet.SeqNr;
            NextExpectedSeq = (ushort) (packet.SeqNr + 1);
            SeqNr = GenerateInitialSeq ();
            ushort synAckSeq = SeqNr;
            SendStateAck (packet);
            SeqNr = (ushort) (synAckSeq + 1);
            State = UtpSessionState.Established;
            Engine.OnSessionEstablished (this);
            ConnectTcs.SetResult (0);
        }

        void HandleState (UtpPacket packet, ReadOnlyMemory<byte> payload)
        {
            // Track the remote's advertised receive window
            if (packet.WindowSize != 0)
                RemoteWindowBytes = (int) Math.Min (int.MaxValue, packet.WindowSize);
            // Outbound handshake completes when we receive a STATE acking our SYN seq
            if (State == UtpSessionState.SynSent) {
                // Acknowledge the remote's sequence number from the state packet
                AckNr = packet.SeqNr;
                NextExpectedSeq = (ushort) (packet.SeqNr + 1);
                State = UtpSessionState.Established;
                Engine.OnSessionEstablished (this);
                ConnectTcs.SetResult (0);
            }

            // Process ACKs for data, including SACK extension if present
            if (State == UtpSessionState.Established) {
                uint sackMask = ParseSackExtension (packet.Extension, payload.Span);
                ProcessAck (packet.AckNr, packet.TimestampDiffMicroseconds, sackMask);
            }
        }

        void HandleData (UtpPacket packet, ReadOnlyMemory<byte> payload)
        {
            // Track the remote's advertised receive window
            if (packet.WindowSize != 0)
                RemoteWindowBytes = (int) Math.Min (int.MaxValue, packet.WindowSize);
            if (payload.Length > 0)
                ReorderBuffer[packet.SeqNr] = payload.ToArray ();

            // Advance contiguous receive window
            bool deliveredAny = false;
            while (ReorderBuffer.TryGetValue (NextExpectedSeq, out var data)) {
                if (data.Length > 0) {
                    if (RxBuffer == null)
                        RxBuffer = new byte[Math.Max (data.Length, 16 * 1024)];
                    if (RxBufferCount + data.Length > RxBuffer.Length) {
                        var n = new byte[Math.Max (RxBufferCount + data.Length, RxBuffer.Length * 2)];
                        System.Buffer.BlockCopy (RxBuffer, 0, n, 0, RxBufferCount);
                        RxBuffer = n;
                    }
                    System.Buffer.BlockCopy (data, 0, RxBuffer, RxBufferCount, data.Length);
                    RxBufferCount += data.Length;
                    deliveredAny = true;
                }
                ReorderBuffer.Remove (NextExpectedSeq);
                AckNr = NextExpectedSeq;
                NextExpectedSeq++;
            }

            // Delayed ACK policy: ack every 2 packets or after AckDelayUs
            PendingAcks++;
            if (PendingAcks >= 2) {
                SendStateAck (packet);
                PendingAcks = 0;
            } else {
                AckDueTimeUs = (uint) (LastPacketArrivalUs + AckDelayUs);
            }

            if (deliveredAny)
                DataAvailableTcs.SetResult (RxBufferCount);
        }

        void HandleFin (UtpPacket packet)
        {
            AckNr = packet.SeqNr;
            SendStateAck (packet);
            FinReceived = true;
            NextExpectedSeq = (ushort) (packet.SeqNr + 1);
            State = UtpSessionState.FinWait;
            if (!FinSent)
                InitiateFin ();
            TryCompleteClose ();
            // Wake any pending readers so they can observe EOF if no data remains
            if (RxBufferCount == 0)
                try { DataAvailableTcs.SetResult (0); } catch { }
        }

        void HandleReset ()
        {
            State = UtpSessionState.Reset;
            Dispose ();
        }

        void SendStateAck (UtpPacket packet)
        {
            try {
                var nowUs = NowMicroseconds ();
                uint tsDiff = LastDelaySampleUs;
                var sackMask = BuildSackMask (AckNr);
                if (sackMask != 0) {
                    var packetBuf = new byte[UtpPacket.HeaderLength + 2 + 4];
                    int written = UtpPacket.Write (packetBuf, UtpPacketType.State, 1, LocalConnIdSend, nowUs, tsDiff, (uint) GetAdvertisedWindow (), SeqNr, AckNr);
                    packetBuf[written + 0] = 0; // next extension = 0 (end)
                    packetBuf[written + 1] = 4; // len
                    packetBuf[written + 2] = (byte) (sackMask >> 24);
                    packetBuf[written + 3] = (byte) (sackMask >> 16);
                    packetBuf[written + 4] = (byte) (sackMask >> 8);
                    packetBuf[written + 5] = (byte) (sackMask);
                    _ = Engine.RequireClient ().SendAsync (packetBuf, packetBuf.Length, RemoteEndPoint);
                } else {
                    var header = new byte[UtpPacket.HeaderLength];
                    int written = UtpPacket.Write (header, UtpPacketType.State, 0, LocalConnIdSend, nowUs, tsDiff, (uint) GetAdvertisedWindow (), SeqNr, AckNr);
                    _ = Engine.RequireClient ().SendAsync (header, written, RemoteEndPoint);
                }
                SeqNr++;
            } catch {
            }
        }

        void SendStateAckInternal (ushort ack)
        {
            try {
                var nowUs = NowMicroseconds ();
                uint tsDiff = LastDelaySampleUs;
                var sackMask = BuildSackMask (ack);
                if (sackMask != 0) {
                    var packetBuf = new byte[UtpPacket.HeaderLength + 2 + 4];
                    int written = UtpPacket.Write (packetBuf, UtpPacketType.State, 1, LocalConnIdSend, nowUs, tsDiff, (uint) GetAdvertisedWindow (), SeqNr, ack);
                    packetBuf[written + 0] = 0; // next extension = 0 (end)
                    packetBuf[written + 1] = 4;
                    packetBuf[written + 2] = (byte) (sackMask >> 24);
                    packetBuf[written + 3] = (byte) (sackMask >> 16);
                    packetBuf[written + 4] = (byte) (sackMask >> 8);
                    packetBuf[written + 5] = (byte) (sackMask);
                    _ = Engine.RequireClient ().SendAsync (packetBuf, packetBuf.Length, RemoteEndPoint);
                } else {
                    var header = new byte[UtpPacket.HeaderLength];
                    int written = UtpPacket.Write (header, UtpPacketType.State, 0, LocalConnIdSend, nowUs, tsDiff, (uint) GetAdvertisedWindow (), SeqNr, ack);
                    _ = Engine.RequireClient ().SendAsync (header, written, RemoteEndPoint);
                }
                SeqNr++;
            } catch {
            }
        }

        void ProcessAck (ushort ackNr, uint tsDiffUs, uint sackMask)
        {
            var nowUs = NowMicroseconds ();

            // Update base delay (sliding window) and queuing delay
            if (tsDiffUs != 0)
                AddBaseDelaySample (tsDiffUs, nowUs);
            var queuingDelayUs = (tsDiffUs > BaseDelayUs) ? (tsDiffUs - BaseDelayUs) : 0u;

            // Mark segments acked and compute RTT on the newest acked
            OutSeg? newestAcked = null;
            for (var node = SendQueue.First; node != null; node = node.Next) {
                var seg = node.Value;
                if (!seg.Sent || seg.Acked)
                    continue;
                if (SeqLE (seg.Seq, ackNr)) {
                    seg.Acked = true;
                    if (BytesInFlight >= seg.Len)
                        BytesInFlight -= seg.Len;
                    newestAcked = seg;
                    if (seg.IsFin)
                        FinAcked = true;
                } else if (sackMask != 0) {
                    // Selective ack for packets within next 32
                    ushort delta = (ushort) (seg.Seq - ackNr);
                    if (delta >= 1 && delta <= 32) {
                        if (((sackMask >> (32 - delta)) & 1) != 0) {
                            seg.Acked = true;
                            if (BytesInFlight >= seg.Len)
                                BytesInFlight -= seg.Len;
                            newestAcked = seg;
                            if (seg.IsFin)
                                FinAcked = true;
                        }
                    }
                }
            }

            // Drop acked from head
            while (SendQueue.First != null && SendQueue.First.Value.Acked)
                SendQueue.RemoveFirst ();

            if (newestAcked != null) {
                uint measuredRtt = (uint) (nowUs - newestAcked.SentAtUs);
                if (SrttUs == 0) {
                    SrttUs = measuredRtt;
                    RttvarUs = measuredRtt / 2;
                } else {
                    uint err = (uint) Math.Abs ((int) (SrttUs - measuredRtt));
                    RttvarUs = (uint) ((3 * RttvarUs + err) / 4);
                    SrttUs = (uint) ((7 * SrttUs + measuredRtt) / 8);
                }
                var rto = SrttUs + Math.Max (MinRtoUs, 4 * RttvarUs);
                if (rto < MinRtoUs) rto = MinRtoUs;
                if (rto > MaxRtoUs) rto = MaxRtoUs;
                RtoUs = rto;

                // LEDBAT-like cwnd update
                int offTarget = TargetDelayUs - (int) queuingDelayUs; // can be negative
                if (offTarget > TargetDelayUs) offTarget = TargetDelayUs;
                if (offTarget < -TargetDelayUs) offTarget = -TargetDelayUs;
                double delta = LedbatGain * MssBytes * ((double) offTarget / Math.Max (1, CwndBytes));
                if (CwndBytes < SsthreshBytes) {
                    // slow start
                    CwndBytes += MssBytes;
                } else {
                    CwndBytes = (int) Math.Max (2 * MssBytes, CwndBytes + delta);
                }
            }

            LastActivityUs = nowUs;
            // Pacing: attempt to send newly allowed data immediately
            TrySendPending (nowUs);
            TryCompleteClose ();

            // Fast retransmit using SACK gaps, or duplicate ACKs
            TryFastRetransmit (ackNr, sackMask, nowUs);

            // Track duplicate ack state after processing
            if (ackNr == LastAckNr && sackMask == LastSackMask)
                DuplicateAckCount++;
            else {
                LastAckNr = ackNr;
                LastSackMask = sackMask;
                DuplicateAckCount = 1;
            }
        }

        void TrySendPending (uint nowUs)
        {
            int sendBudget = Math.Min (CwndBytes, RemoteWindowBytes);
            for (var node = SendQueue.First; node != null; node = node.Next) {
                var seg = node.Value;
                if (seg.Acked)
                    continue;
                if (!seg.Sent) {
                    if (BytesInFlight + seg.Len > sendBudget)
                        break;
                    TransmitSegment (seg, nowUs);
                }
            }
        }

        void TryFastRetransmit (ushort ackNr, uint sackMask, uint nowUs)
        {
            bool lossDetected = false;

            // SACK-based: if a higher seq is SACKed and a lower outstanding segment within the mask window is missing, retransmit it.
            if (sackMask != 0) {
                for (var node = SendQueue.First; node != null; node = node.Next) {
                    var seg = node.Value;
                    if (!seg.Sent || seg.Acked || seg.FastRetxDone)
                        continue;
                    ushort delta = (ushort) (seg.Seq - ackNr);
                    if (delta >= 1 && delta <= 32) {
                        bool thisAcked = ((sackMask >> (32 - delta)) & 1) != 0;
                        if (!thisAcked) {
                            uint higherMask = sackMask << delta; // any higher seq acked?
                            if (higherMask != 0) {
                                TransmitSegment (seg, nowUs);
                                seg.FastRetxDone = true;
                                lossDetected = true;
                                break;
                            }
                        }
                    }
                }
            }

            // Dup-ack heuristic: if 3+ duplicate ACKs with identical state, retransmit first outstanding segment.
            if (!lossDetected && DuplicateAckCount >= 3) {
                for (var node = SendQueue.First; node != null; node = node.Next) {
                    var seg = node.Value;
                    if (!seg.Sent || seg.Acked)
                        continue;
                    TransmitSegment (seg, nowUs);
                    seg.FastRetxDone = true;
                    lossDetected = true;
                    DuplicateAckCount = 0;
                    break;
                }
            }

            if (lossDetected) {
                CwndBytes = Math.Max (2 * MssBytes, CwndBytes / 2);
                SsthreshBytes = CwndBytes;
            }
        }

        void TransmitSegment (OutSeg seg, uint nowUs)
        {
            var tsDiff = LastDelaySampleUs;
            if (seg.IsFin) {
                var header = new byte[UtpPacket.HeaderLength];
                var written = UtpPacket.Write (header, UtpPacketType.Fin, 0, LocalConnIdSend, nowUs, tsDiff, (uint) GetAdvertisedWindow (), seg.Seq, AckNr);
                _ = Engine.RequireClient ().SendAsync (header, written, RemoteEndPoint);
            } else {
                var header = new byte[UtpPacket.HeaderLength + seg.Len];
                var written = UtpPacket.Write (header, UtpPacketType.Data, 0, LocalConnIdSend, nowUs, tsDiff, (uint) GetAdvertisedWindow (), seg.Seq, AckNr);
                if (seg.Len > 0)
                    System.Buffer.BlockCopy (seg.Data, 0, header, written, seg.Len);
                _ = Engine.RequireClient ().SendAsync (header, written + seg.Len, RemoteEndPoint);
            }

            if (!seg.Sent)
                BytesInFlight += seg.Len;
            seg.Sent = true;
            seg.SentAtUs = nowUs;
            seg.DeadlineUs = RtoUs;
            LastActivityUs = nowUs;
        }

        static bool SeqLE (ushort a, ushort b)
            => (ushort) (b - a) < 0x8000;

        void AddBaseDelaySample (uint tsDiffUs, uint nowUs)
        {
            // Insert sample into circular buffer
            BaseDelayValues[BaseDelayIndex] = tsDiffUs;
            BaseDelayTimes[BaseDelayIndex] = nowUs;
            BaseDelayIndex = (BaseDelayIndex + 1) % BaseDelaySampleCount;
            if (BaseDelayUsed < BaseDelaySampleCount)
                BaseDelayUsed++;

            // Recompute minimum over samples within the time window
            uint min = uint.MaxValue;
            int count = BaseDelayUsed;
            for (int i = 0; i < count; i++) {
                uint t = BaseDelayTimes[i];
                if ((uint) (nowUs - t) <= BaseDelayWindowUs) {
                    uint v = BaseDelayValues[i];
                    if (v < min)
                        min = v;
                }
            }
            // If none in window (e.g. just started), fall back to min over all samples
            if (min == uint.MaxValue) {
                for (int i = 0; i < count; i++) {
                    uint v = BaseDelayValues[i];
                    if (v < min)
                        min = v;
                }
            }
            if (min != uint.MaxValue)
                BaseDelayUs = min;
        }

        // FIN/RESET
        bool FinSent;
        bool FinAcked;
        bool FinReceived;
        ushort FinSeq;

        internal bool IsClosingGracefully => FinSent || FinReceived;

        internal void InitiateFin ()
        {
            if (FinSent || State == UtpSessionState.Reset || State == UtpSessionState.Closed)
                return;

            FinSeq = SeqNr++;
            var fin = new OutSeg { Seq = FinSeq, Len = 0, IsFin = true };
            SendQueue.AddLast (fin);
            FinSent = true;
            State = UtpSessionState.FinWait;
            TrySendPending (NowMicroseconds ());
        }

        internal void SendReset ()
        {
            try {
                var nowUs = NowMicroseconds ();
                var header = new byte[UtpPacket.HeaderLength];
                UtpPacket.Write (header, UtpPacketType.Reset, 0, LocalConnIdSend, nowUs, 0, (uint) GetAdvertisedWindow (), SeqNr, AckNr);
                Engine.RequireClient ().Send (header, header.Length);
            } catch { }
            State = UtpSessionState.Reset;
            CloseSession ();
        }

        void TryCompleteClose ()
        {
            if (FinReceived && FinAcked)
                CloseSession ();
        }

        void CloseSession ()
        {
            State = UtpSessionState.Closed;
            Engine.RemoveSession (this);
            SignalClosed ();
        }

        void SignalClosed ()
        {
            if (CloseSignalled)
                return;
            CloseSignalled = true;
            try { CloseTcs.SetResult (true); } catch { }
        }

        int GetAdvertisedWindow ()
        {
            int avail = RxWindowBytes - RxBufferCount;
            if (avail < 0)
                avail = 0;
            return avail;
        }

        static uint ParseSackExtension (byte firstExtension, ReadOnlySpan<byte> span)
        {
            int offset = 0;
            byte current = firstExtension;
            while (current != 0 && offset + 2 <= span.Length) {
                byte next = span[offset];
                byte len = span[offset + 1];
                offset += 2;
                if (offset + len > span.Length)
                    break;
                if (current == 1) {
                    int l = len < 4 ? len : 4;
                    uint mask = 0;
                    for (int i = 0; i < l; i++)
                        mask = (mask << 8) | span[offset + i];
                    return mask;
                }
                offset += len;
                current = next;
            }
            return 0;
        }

        static int GetExtensionsLength (byte firstExtension, ReadOnlySpan<byte> span)
        {
            int offset = 0;
            byte current = firstExtension;
            while (current != 0 && offset + 2 <= span.Length) {
                byte next = span[offset];
                byte len = span[offset + 1];
                offset += 2;
                if (offset + len > span.Length)
                    break;
                offset += len;
                current = next;
            }
            return offset;
        }

        uint BuildSackMask (ushort ack)
        {
            uint mask = 0;
            for (int i = 1; i <= 32; i++) {
                ushort seq = (ushort) (ack + i);
                if (ReorderBuffer.ContainsKey (seq))
                    mask |= (1u << (32 - i));
            }
            return mask;
        }

        static uint NowMicroseconds ()
        {
            // Monotonic timestamp in microseconds based on Stopwatch for broad TFMs.
            double us = Stopwatch.GetTimestamp () * (1_000_000.0 / Stopwatch.Frequency);
            unchecked { return (uint) ((ulong) us % uint.MaxValue); }
        }
    }
}
