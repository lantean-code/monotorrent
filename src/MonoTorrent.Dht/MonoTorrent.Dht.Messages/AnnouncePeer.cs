//
// AnnouncePeer.cs
//
// Authors:
//   Alan McGovern <alan.mcgovern@gmail.com>
//
// Copyright (C) 2008 Alan McGovern
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
using System.Linq;
using System.Net.Sockets;

using MonoTorrent.BEncoding;
using MonoTorrent.Logging;

namespace MonoTorrent.Dht.Messages
{
    internal sealed class AnnouncePeer : QueryMessage
    {
        private static ILogger Logger = LoggerFactory.Create (nameof (AnnouncePeer));

        private static readonly BEncodedString InfoHashKey = new BEncodedString ("info_hash");
        private static readonly BEncodedString QueryName = new BEncodedString ("announce_peer");
        private static readonly BEncodedString PortKey = new BEncodedString ("port");
        private static readonly BEncodedString TokenKey = new BEncodedString ("token");
        private static readonly BEncodedString ImpliedPortKey = new BEncodedString ("implied_port");

        internal NodeId InfoHash => new NodeId ((BEncodedString) Parameters[InfoHashKey]);

        internal BEncodedNumber Port => (BEncodedNumber) Parameters[PortKey];

        internal BEncodedNumber? ImpliedPort => (BEncodedNumber?) Parameters.GetValueOrDefault (ImpliedPortKey);

        internal BEncodedString Token => (BEncodedString) Parameters[TokenKey];

        public AnnouncePeer (NodeId id, NodeId infoHash, BEncodedNumber port, BEncodedValue token)
            : base (id, QueryName)
        {
            Parameters.Add (InfoHashKey, BEncodedString.FromMemory (infoHash.AsMemory ()));
            Parameters.Add (PortKey, port);
            Parameters.Add (TokenKey, token);
        }

        public AnnouncePeer (BEncodedDictionary d)
            : base (d)
        {
        }

        public override ResponseMessage CreateResponse (BEncodedDictionary parameters)
        {
            return new AnnouncePeerResponse (parameters);
        }

        public override void Handle (DhtEngine engine, Node node)
        {
            base.Handle (engine, node);

            if (!engine.Torrents.ContainsKey (InfoHash))
                engine.Torrents.Add (InfoHash, new List<Node> ());

            if (TransactionId is null) {
                Logger.Error ("Transaction id was unexpectedly missing");
                return;
            }

            if (engine.TokenManager.VerifyToken (node, Token)) {
                var response = new AnnouncePeerResponse (engine.RoutingTable.LocalNodeId, TransactionId);
                engine.MessageLoop.EnqueueSend (response, node, node.EndPoint);

                if ((engine.Capabilities & DhtCapabilities.StoreAnnouncedPeers) != 0) {
                    engine.Torrents[InfoHash].Add (node);
                }

                if ((engine.Capabilities & DhtCapabilities.EmitAnnouncePeersAsFound) != 0) {
                    int peerPort;
                    if (ImpliedPort?.Number == 1) {
                        peerPort = node.EndPoint.Port;
                    } else {
                        if (!TryGetPort (Parameters, out peerPort))
                            peerPort = 0; // invalid, will skip emit below
                    }

                    var ip4 = node.EndPoint.Address.MapToIPv4 ();

                    if (ip4.AddressFamily == AddressFamily.InterNetwork && peerPort > 0 && peerPort <= 65535) {
                        Span<byte> buffer = stackalloc byte[6];
                        WriteCompactPeer (peerPort, ip4, buffer);

                        var peers = PeerInfo.FromCompact (buffer, engine.AddressFamily).ToArray ();
                        if (peers.Length > 0)
                            engine.RaisePeersFound (InfoHash, peers);
                    }
                }
            } else {
                var errorResponse = new ErrorMessage (TransactionId, ErrorCode.ProtocolError, "Invalid or expired token received");
                engine.MessageLoop.EnqueueSend (errorResponse, node, node.EndPoint);
            }
        }

        private static void WriteCompactPeer (int peerPort, System.Net.IPAddress ip4, Span<byte> buffer)
        {
            var ipBytes = ip4.GetAddressBytes ();
            buffer[0] = ipBytes[0];
            buffer[1] = ipBytes[1];
            buffer[2] = ipBytes[2];
            buffer[3] = ipBytes[3];
            buffer[4] = (byte) (peerPort >> 8);
            buffer[5] = (byte) (peerPort & 0xFF);
        }

        private static bool TryGetPort (BEncodedDictionary p, out int port)
        {
            if (p.TryGetValue (PortKey, out var v) && v is BEncodedNumber n) {
                port = (int) n.Number;
                return port > 0 && port <= 65535;
            }
            port = 0;
            return false;
        }
    }
}
