//
// SampleInfoHashes.cs
//
// Authors:
//   Alex Jephson <alex@example.com> (modeled after MonoTorrent style)
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to do so, subject to the following conditions:
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
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Security.Cryptography;

using MonoTorrent.BEncoding;
using MonoTorrent.Logging;

namespace MonoTorrent.Dht.Messages
{
    sealed class SampleInfoHashes : QueryMessage
    {
        static ILogger Logger = LoggerFactory.Create (nameof (SampleInfoHashes));

        static readonly BEncodedString QueryName = new BEncodedString ("sample_infohashes");
        static readonly BEncodedString TargetKey = new BEncodedString ("target");
        static readonly BEncodedString NKey = new BEncodedString ("n");

        const int DefaultIntervalSeconds = 600;
        const int DefaultSampleCount = 100;

        public NodeId Target => new NodeId ((BEncodedString) Parameters[TargetKey]);

        public BEncodedNumber? N {
            get => (BEncodedNumber?) Parameters.GetValueOrDefault (NKey);
            set {
                if (value is null)
                    Parameters.Remove (NKey);
                else
                    Parameters[NKey] = value;
            }
        }

        public SampleInfoHashes (NodeId id, NodeId target, int? n = null)
            : base (id, QueryName)
        {
            Parameters.Add (TargetKey, BEncodedString.FromMemory (target.AsMemory ()));
            if (n.HasValue)
                Parameters.Add (NKey, new BEncodedNumber (n.Value));
        }

        public SampleInfoHashes (BEncodedDictionary d)
            : base (d)
        {
        }

        public override ResponseMessage CreateResponse (BEncodedDictionary parameters)
        {
            return new SampleInfoHashesResponse (parameters);
        }

        public override void Handle (DhtEngine engine, Node node)
        {
            base.Handle (engine, node);

            if (TransactionId is null) {
                Logger.Error ("Transaction id was unexpectedly missing");
                return;
            }

            var response = new SampleInfoHashesResponse (engine.RoutingTable.LocalNodeId, TransactionId) {
                Nodes = Node.CompactNode (engine.RoutingTable.GetClosest (Target))
            };

            if ((engine.Capabilities & DhtCapabilities.ServeSamples) != 0) {
                int requested = (int?) N?.Number ?? DefaultSampleCount;

                var keys = engine.Torrents.Keys;
                List<NodeId> nodeIds;
                if (engine.SamplingAlgorithm == SamplingAlgorithm.HashOrder) {
                    nodeIds = SelectByHashOrder (keys, requested, engine.Sampling.GetSalt());
                } else if (engine.SamplingAlgorithm == SamplingAlgorithm.Reservoir) {
                    nodeIds = ReservoirSample (keys, requested, engine.Sampling.CreateIntervalRandom());
                } else {
                    throw new InvalidOperationException ($"Unknown sampling algorithm: {engine.SamplingAlgorithm}");
                }

                var samples = ConcatSamples (nodeIds);

                if (samples != null) {
                    response.Samples = samples;
                    response.Num = new BEncodedNumber (nodeIds.Count);
                    response.Interval = new BEncodedNumber (DefaultIntervalSeconds);
                }
            }

            engine.MessageLoop.EnqueueSend (response, node, node.EndPoint);
        }

        static List<NodeId> SelectByHashOrder (IEnumerable<NodeId> keys, int k, byte[] salt)
        {
            if (k <= 0) {
                return new List<NodeId> (0);
            }

            var reservoir = new List<(ulong Score, NodeId Key)> (k);
            ulong maxScore = 0;
            int maxIndex = -1;

            foreach (var key in keys) {
                var score = ComputeScore (key, salt);

                if (reservoir.Count < k) {
                    reservoir.Add ((score, key));

                    if (score >= maxScore) {
                        maxScore = score;
                        maxIndex = reservoir.Count - 1;
                    }
                } else if (score < maxScore) {
                    reservoir[maxIndex] = (score, key);
                    (maxScore, maxIndex) = FindMax (reservoir);
                }
            }

            var result = new List<NodeId> (reservoir.Count);
            for (var i = 0; i < reservoir.Count; i++) {
                result.Add (reservoir[i].Key);
            }

            return result;

            static (ulong Max, int Index) FindMax (List<(ulong Score, NodeId Key)> list)
            {
                ulong currentMax = 0;
                var currentIdx = -1;

                for (var i = 0; i < list.Count; i++) {
                    if (list[i].Score >= currentMax) {
                        currentMax = list[i].Score;
                        currentIdx = i;
                    }
                }

                return (currentMax, currentIdx);
            }

            static ulong ComputeScore (NodeId key, byte[] salt)
            {
                if (key is null) {
                    throw new ArgumentNullException (nameof (key));
                }

                if (salt is null) {
                    throw new ArgumentNullException (nameof (salt));
                }

                // Concatenate salt || key (20 bytes)
                var keyBytes = key.AsMemory ().ToArray (); // NodeId → 20 bytes
                var buffer = new byte[salt.Length + keyBytes.Length];
                Buffer.BlockCopy (salt, 0, buffer, 0, salt.Length);
                Buffer.BlockCopy (keyBytes, 0, buffer, salt.Length, keyBytes.Length);

                byte[] hash;
                using (var sha1 = SHA1.Create ()) {
                    hash = sha1.ComputeHash (buffer); // 20 bytes
                }

                // Take the first 8 bytes as an unsigned big-endian integer
                ulong score = 0UL;
                for (int i = 0; i < 8; i++) {
                    score = (score << 8) | hash[i];
                }

                return score;
            }
        }

        static List<NodeId> ReservoirSample (IEnumerable<NodeId> keys, int k, Random rng)
        {
            if (k <= 0) {
                return new List<NodeId> (0);
            }

            var reservoir = new List<NodeId> (k);
            var i = 0;

            foreach (var key in keys) {
                if (reservoir.Count < k) {
                    reservoir.Add (key);
                } else {
                    var j = rng.Next (++i + (k - reservoir.Count));
                    if (j < k) {
                        reservoir[j] = key;
                    }
                }

                i++;
            }

            return reservoir;
        }

        static BEncodedString ConcatSamples (List<NodeId> selected)
        {
            if (selected is null) {
                throw new ArgumentNullException (nameof (selected));
            }

            if (selected.Count == 0) {
                // BEP-51 allows zero-length samples; return an empty string rather than null.
                return BEncodedString.FromMemory (Array.Empty<byte> ());
            }

            const int InfoHashSize = 20;
            var buffer = new byte[selected.Count * InfoHashSize];
            var offset = 0;

            for (int i = 0; i < selected.Count; i++) {
                // NodeId is 20 bytes; copy into the buffer.
                var keyBytes = selected[i].AsMemory ().ToArray ();
                if (keyBytes.Length != InfoHashSize) {
                    throw new ArgumentException ("NodeId must be exactly 20 bytes.", nameof (selected));
                }

                Buffer.BlockCopy (keyBytes, 0, buffer, offset, InfoHashSize);
                offset += InfoHashSize;
            }

            return BEncodedString.FromMemory (buffer);
        }
    }
}
