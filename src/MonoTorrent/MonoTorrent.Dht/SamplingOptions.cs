using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace MonoTorrent.Dht
{
    public sealed class SamplingOptions
    {
        // Workload / budget
        /// <summary>
        /// Total infohashes to emit this round.
        /// </summary>
        public int MaxSamples { get; set; }            // total infohashes to emit this round

        /// <summary>
        /// Total nodes to query across the round.
        /// </summary>
        public int MaxFanout { get; set; }             // total nodes to query across the round

        /// <summary>
        /// In-flight requests at any one time.
        /// </summary>
        public int MaxConcurrency { get; set; }        // in-flight sample_infohashes RPCs

        /// <summary>
        /// 'n' hint sent in each BEP-51 request.
        /// </summary>
        public int PerNodeRequestN { get; set; }       // 'n' hint sent in each BEP-51 request

        // Target generation (what keys to sample around)
        /// <summary>
        /// The strategy used to generate target keys.
        /// </summary>
        public TargetStrategy TargetStrategy { get; set; }

        /// <summary>
        /// How many distinct targets to probe.
        /// </summary>
        public int TargetCount { get; set; }           // how many distinct targets to probe

        // Node selection (which nodes to ask)
        public NodeSelection NodeSelection { get; set; }

        public int PerBucketHeadCount { get; set; } = 1; // nodes per bucket

        // For ReputationWeighted. Higher is better. If null, fallback to ClosestToTarget.
        public Func<DhtNodeContact, double, bool>? NodeReputationScore { get; set; }

        // Optional seed nodes (BEP-5 compact 26-byte entries); engine merges with routing table
        /// <summary>
        /// Optional seed nodes (BEP-5 compact 26-byte entries); engine merges with routing table.
        /// </summary>
        public IReadOnlyList<byte[]>? SeedCompactNodes { get; set; }

        // Politeness / pacing
        /// <summary>
        /// Honour 'interval' from responses.
        /// </summary>
        public bool RespectInterval { get; set; }      // honour 'interval' from responses

        /// <summary>
        /// Minimum time before re-querying the same node.
        /// </summary>
        public TimeSpan PerNodeCooldown { get; set; }  // min time before re-querying same node

        /// <summary>
        /// Hard stop for the round.
        /// </summary>
        public TimeSpan GlobalDeadline { get; set; }   // hard stop for the round

        /// <summary>
        /// Raise event with source nodes for each infohash.
        /// </summary>
        public bool EmitSources { get; set; }      // raise event with source nodes for each infohash

        /// <summary>
        /// Maximum sources to keep per infohash if EmitSources is true.
        /// </summary>
        public int MaxSourcesPerHash { get; set; } = 8; // max sources to keep per infohash if EmitSources is true
    }

    public enum TargetStrategy
    {
        /// <summary>
        /// Random 160-bit keys (default).
        /// </summary>
        Random,        // random 160-bit keys

        /// <summary>
        /// Generate targets to cover buckets evenly.
        /// </summary>
        BucketSweep,   // generate targets to cover buckets evenly
    }

    public enum NodeSelection
    {
        /// <summary>
        /// Kademlia walk (default).
        /// </summary>
        ClosestToTarget,

        /// <summary>
        /// 1..N nodes per bucket for diversity.
        /// </summary>
        PerBucketHead,

        /// <summary>
        /// Prefer nodes that yielded more samples previously.
        /// </summary>
        ReputationWeighted
    }
}
