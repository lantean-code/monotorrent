//
// SampleInfoHashesTask.cs
//
// Authors:
//   Alan McGovern alan.mcgovern@gmail.com
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
using System.Threading;
using System.Threading.Tasks;

using MonoTorrent.BEncoding;
using MonoTorrent.Dht.Messages;

namespace MonoTorrent.Dht.Tasks
{
    internal class SampleInfoHashesTask
    {
        private const int EmitChunkSize = 200;

        private readonly SamplingOptions Options;

        // Results / de-dupe
        private readonly Dictionary<NodeId, HashSet<Node>> FoundSamples;

        private HashSet<Node> QueriedNodes { get; }

        // Pacing (BEP-51 interval + optional floor)
        private Dictionary<Node, DateTime> NodeNextAllowed { get; }

        // Error handling/backoff state
        private readonly HashSet<Node> Unsupported = new HashSet<Node> ();

        // BEP-51 '204' responders
        private readonly Dictionary<Node, DateTime> BackoffUntil = new Dictionary<Node, DateTime> ();

        private readonly Dictionary<Node, int> ErrorStrikes = new Dictionary<Node, int> ();

        private const int MaxServerErrorStrikes = 2;
        private static readonly TimeSpan ShortBackoff = TimeSpan.FromMinutes (2);
        private static readonly TimeSpan LongBackoff = TimeSpan.FromMinutes (15);
        private static readonly TimeSpan ProtoBackoff = TimeSpan.FromMinutes (30);

        // Budgets
        private int TotalFanout;

        private DhtEngine Engine { get; }

        public SampleInfoHashesTask (DhtEngine engine, SamplingOptions options)
        {
            Engine = engine;
            Options = options;
            Normalize (Options);

            FoundSamples = new Dictionary<NodeId, HashSet<Node>> ();
            QueriedNodes = new HashSet<Node> ();
            NodeNextAllowed = new Dictionary<Node, DateTime> ();
        }

        public async Task<IEnumerable<NodeId>> ExecuteAsync (CancellationToken cancellationToken = default)
        {
            DhtEngine.MainLoop.CheckThread ();

            // Prepare targets
            var targets = GenerateTargets (Options.TargetStrategy, Options.TargetCount).ToArray ();
            if (targets.Length == 0)
                targets = new[] { NodeId.Create () };

            // Track closest nodes per target (same style as GetPeersTask)
            var perTargetClosestNodes = new Dictionary<NodeId, ClosestNodesCollection> (targets.Length);
            foreach (var t in targets)
                perTargetClosestNodes[t] = new ClosestNodesCollection (t);

            var activeQueries = new List<Task<SendQueryEventArgs>> ();

            // Seed initial fanout from the first target
            foreach (Node node in SelectInitialNodes (targets[0])) {
                if (perTargetClosestNodes[targets[0]].Add (node) && ShouldQuery (node)) {
                    if (!TryBudgetEnqueue ())
                        break;
                    activeQueries.Add (Engine.SendQueryAsync (NewQuery (targets[0]), node));
                }
                if (activeQueries.Count >= Options.MaxConcurrency)
                    break;
            }

            while (activeQueries.Count > 0 && !BudgetsExceeded () && !cancellationToken.IsCancellationRequested) {
                var completed = await Task.WhenAny (activeQueries);
                activeQueries.Remove (completed);

                // If it timed out or failed, move on
                SendQueryEventArgs query = await completed;

                // error replies steer our backoff/unsupported sets
                if (query.Error != null) {
                    HandleError (query.Node, query.Error);
                    continue;
                }

                if (query.Response == null)
                    continue;

                var response = (SampleInfoHashesResponse) query.Response;

                // Respect BEP-51 'interval' and an optional minimum cooldown
                if (Options.RespectInterval && response.Interval != null) {
                    var seconds = (int) (response.Interval).Number;
                    if (seconds > 0) {
                        var next = DateTime.UtcNow.AddSeconds (seconds);
                        if (Options.PerNodeCooldown > TimeSpan.Zero && next < DateTime.UtcNow + Options.PerNodeCooldown)
                            next = DateTime.UtcNow + Options.PerNodeCooldown;
                        NodeNextAllowed[query.Node] = next;
                    }
                } else if (Options.PerNodeCooldown > TimeSpan.Zero) {
                    NodeNextAllowed[query.Node] = DateTime.UtcNow + Options.PerNodeCooldown;
                }

                // Harvest samples
                if (response.Samples != null && FoundSamples.Count < Options.MaxSamples) {
                    foreach (var sample in ParseSamples (response.Samples)) {
                        if (!FoundSamples.TryGetValue (sample, out var set)) {
                            set = new HashSet<Node> ();
                            FoundSamples[sample] = set;
                        }
                        if (Options.EmitSources) {
                            if (Options.MaxSourcesPerHash <= 0 || set.Count < Options.MaxSourcesPerHash) {
                                // under cap (or uncapped) -> just add
                                set.Add (query.Node);
                            } else if (!set.Contains (query.Node)) {
                                // at cap: keep the closest 'MaxSourcesPerHash' by XOR distance
                                var worst = GetFurthestByXor (set, sample);
                                if (IsCloser (query.Node, worst, sample)) {
                                    set.Remove (worst);
                                    set.Add (query.Node);
                                }
                            }
                        }

                        if (FoundSamples.Count >= Options.MaxSamples)
                            break;
                    }
                }

                if (BudgetsExceeded () || cancellationToken.IsCancellationRequested)
                    break;

                // Expand: query closer nodes for this target
                if (response.Nodes != null) {
                    var currentTarget = SelectClosestTarget (perTargetClosestNodes, query.Node);

                    foreach (Node node in Node.FromCompactNode (response.Nodes)) {
                        if (FoundSamples.Count >= Options.MaxSamples)
                            break;

                        if (!ShouldQuery (node))
                            continue;

                        if (perTargetClosestNodes[currentTarget].Add (node)) {
                            if (!TryBudgetEnqueue ())
                                break;

                            activeQueries.Add (Engine.SendQueryAsync (NewQuery (currentTarget), node));
                            if (activeQueries.Count >= Options.MaxConcurrency)
                                break;
                        }
                    }
                }

                // If idle and under budget, prime more from other targets
                if (activeQueries.Count == 0 && !BudgetsExceeded ()) {
                    for (int i = 0; i < targets.Length; i++) {
                        var t = targets[i];
                        foreach (Node node in SelectInitialNodes (t)) {
                            if (perTargetClosestNodes[t].Add (node) && ShouldQuery (node)) {
                                if (!TryBudgetEnqueue ())
                                    break;

                                activeQueries.Add (Engine.SendQueryAsync (NewQuery (t), node));
                            }
                            if (activeQueries.Count >= Options.MaxConcurrency || BudgetsExceeded ())
                                break;
                        }
                        if (activeQueries.Count > 0 || BudgetsExceeded ())
                            break;
                    }
                }
            }

            // Final flush
            if (FoundSamples.Count > 0)
                Engine.RaiseInfoHashesFound (FoundSamples);

            return FoundSamples.Keys;
        }

        // ——— helpers ———

        private QueryMessage NewQuery (NodeId target)
        {
            var n = Options.PerNodeRequestN > 0 ? Options.PerNodeRequestN : 100;
            return new SampleInfoHashes (Engine.LocalId, target, n);
        }

        private bool ShouldQuery (Node node)
        {
            if (QueriedNodes.Contains (node))
                return false;

            // Permanent exclude: node claims it doesn't support BEP-51
            if (Unsupported.Contains (node))
                return false;

            // Cooldowns: BEP-51 interval limits, our user-configured cooldowns, and error backoffs
            if (Options.RespectInterval || Options.PerNodeCooldown > TimeSpan.Zero || BackoffUntil.Count > 0) {
                DateTime nextAllowed;
                if (NodeNextAllowed.TryGetValue (node, out nextAllowed) && DateTime.UtcNow < nextAllowed)
                    return false;
                if (BackoffUntil.TryGetValue (node, out nextAllowed) && DateTime.UtcNow < nextAllowed)
                    return false;
            }

            QueriedNodes.Add (node);
            return true;
        }

        private bool TryBudgetEnqueue ()
        {
            if (TotalFanout >= Options.MaxFanout)
                return false;
            TotalFanout++;
            return true;
        }

        private static IEnumerable<NodeId> ParseSamples (BEncodedString concatenated)
        {
            var mem = concatenated.AsMemory ();
            int count = mem.Length / 20;

            for (int i = 0; i < count; i++) {
                var slice = mem.Slice (i * 20, 20);
                yield return NodeId.FromMemory (slice);
            }
        }

        private IEnumerable<NodeId> GenerateTargets (TargetStrategy strategy, int count)
        {
            if (count <= 0)
                count = 1;

            switch (strategy) {
                case TargetStrategy.Random:
                default:
                    for (int i = 0; i < count; i++)
                        yield return NodeId.Create ();
                    break;

                case TargetStrategy.BucketSweep:
                    var min = NodeId.Minimum;
                    var max = NodeId.Maximum;

                    var minBig = new BigEndianBigInteger (min.Span);
                    var maxBig = new BigEndianBigInteger (max.Span);
                    var range = maxBig - minBig;
                    var step = range / (count + 1);

                    for (int i = 1; i <= count; i++) {
                        var value = minBig + (step * i);
                        yield return new NodeId (value);
                    }
                    break;
            }
        }

        private NodeId SelectClosestTarget (Dictionary<NodeId, ClosestNodesCollection> perTargetClosest, Node node)
        {
            foreach (var kvp in perTargetClosest) {
                if (kvp.Value.Contains (node))
                    return kvp.Key;
            }
            foreach (var kvp in perTargetClosest)
                return kvp.Key;
            return NodeId.Minimum;
        }

        private void Normalize (SamplingOptions o)
        {
            if (o.MaxSamples <= 0)
                o.MaxSamples = 5000;
            if (o.MaxFanout <= 0)
                o.MaxFanout = 256;
            if (o.MaxConcurrency <= 0)
                o.MaxConcurrency = 16;
            if (o.PerNodeRequestN <= 0)
                o.PerNodeRequestN = 100;
            if (o.TargetCount <= 0)
                o.TargetCount = 8;
            if (o.PerNodeCooldown < TimeSpan.Zero)
                o.PerNodeCooldown = TimeSpan.Zero;
        }

        private bool BudgetsExceeded ()
            => FoundSamples.Count >= Options.MaxSamples || TotalFanout >= Options.MaxFanout;

        // NEW: centralised error handling/backoff
        private void HandleError (Node node, ErrorMessage err)
        {
            switch (err.ErrorCode) {
                case ErrorCode.MethodUnknown: // 204 — does not implement BEP-51
                    Unsupported.Add (node);
                    BackoffUntil[node] = DateTime.UtcNow + TimeSpan.FromDays (7);
                    break;

                case ErrorCode.ServerError:   // 202 — transient/throttling
                    int strikes;
                    ErrorStrikes.TryGetValue (node, out strikes);
                    strikes++;
                    ErrorStrikes[node] = strikes;

                    BackoffUntil[node] = DateTime.UtcNow + (strikes >= MaxServerErrorStrikes ? LongBackoff : ShortBackoff);
                    break;

                case ErrorCode.ProtocolError: // 203 — bad args / too-large 'n'
                    BackoffUntil[node] = DateTime.UtcNow + ProtoBackoff;
                    break;

                default:                      // 201 or anything else
                    BackoffUntil[node] = DateTime.UtcNow + ShortBackoff;
                    break;
            }
        }

        private static bool IsCloser (Node a, Node b, NodeId target)
            => (target ^ a.Id).CompareTo (target ^ b.Id) < 0;

        private static Node GetFurthestByXor (HashSet<Node> set, NodeId target)
        {
            Node? furthest = null;
            foreach (var n in set) {
                if (furthest == null || (target ^ n.Id).CompareTo (target ^ furthest.Id) > 0)
                    furthest = n;
            }
            return furthest!;
        }

        private IEnumerable<Node> SelectInitialNodes (NodeId target)
        {
            // Resolve selection mode (fallback if no scorer provided)
            var selection = Options.NodeSelection;
            var scorer = Options.NodeReputationScore;
            if (selection == NodeSelection.ReputationWeighted && scorer == null)
                selection = NodeSelection.ClosestToTarget;

            // Guard against accidental duplicates (belt-and-braces)
            var yielded = new HashSet<Node> ();

            switch (selection) {
                case NodeSelection.PerBucketHead: {
                    // Diversity: pick 1..N “head” nodes per bucket
                    int perBucket = Options.PerBucketHeadCount > 0 ? Options.PerBucketHeadCount : 1;

                    foreach (var bucket in Engine.RoutingTable.Buckets) {
                        int taken = 0;

                        foreach (var n in bucket.Nodes) {
                            if ((n.State == NodeState.Good || n.State == NodeState.Questionable) && yielded.Add (n)) {
                                yield return n;
                                taken++;
                                if (taken >= perBucket)
                                    break;
                            }
                        }

                        // Fallback to a replacement if bucket had nothing suitable
                        if (taken == 0 && bucket.Replacement != null && yielded.Add (bucket.Replacement))
                            yield return bucket.Replacement;
                    }
                    break;
                }

                case NodeSelection.ReputationWeighted: {
                    // Bias toward historically high-yield nodes; cap sort work
                    var all = Engine.RoutingTable.Buckets.SelectMany (b => b.Nodes);
                    int take = Math.Max (Options.MaxConcurrency * 4, 32);

                    foreach (var n in all
                        .OrderByDescending (n => scorer! (n.EndPoint, n.Id.AsMemory ()))
                        .ThenBy (n => target ^ n.Id) // stable tie-breaker by XOR distance
                        .Take (take)) {
                        if (yielded.Add (n))
                            yield return n;
                    }
                    break;
                }

                case NodeSelection.ClosestToTarget:
                default: {
                    foreach (var n in Engine.RoutingTable.GetClosest (target)) {
                        if (yielded.Add (n))
                            yield return n;
                    }
                    break;
                }
            }
        }

    }
}
