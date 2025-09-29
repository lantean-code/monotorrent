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
using System.Collections.Generic;

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
                var samples = ConcatSamples (keys, requested);

                if (samples != null) {
                    response.Samples = samples;
                    response.Num = new BEncodedNumber (keys.Count);
                    response.Interval = new BEncodedNumber (DefaultIntervalSeconds);
                }
            }

            engine.MessageLoop.EnqueueSend (response, node, node.EndPoint);
        }

        static BEncodedString? ConcatSamples (ICollection<NodeId> keys, int max)
        {
            if (keys.Count == 0 || max <= 0)
                return null;

            int take = Math.Min (max, keys.Count);
            int stride = Math.Max (1, keys.Count / take);

            var buffer = new byte[take * 20];
            int i = 0, written = 0;

            foreach (var k in keys) {
                if ((i++ % stride) != 0)
                    continue;

                var span = k.AsMemory ().Span;
                Buffer.BlockCopy (span.ToArray (), 0, buffer, written * 20, 20);

                if (++written == take)
                    break;
            }

            if (written == 0)
                return null;

            if (written != take) {
                var trimmed = new byte[written * 20];
                Buffer.BlockCopy (buffer, 0, trimmed, 0, trimmed.Length);
                return BEncodedString.FromMemory (trimmed);
            }

            return BEncodedString.FromMemory (buffer);
        }
    }
}
