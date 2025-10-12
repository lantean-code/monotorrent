using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MonoTorrent.Dht
{
    internal sealed class ReputationTracker
    {
        // MainLoop-confined; no extra locking needed if only touched on MainLoop.
        readonly Dictionary<(System.Net.IPEndPoint ep, NodeId id), Stats> _map
            = new Dictionary<(System.Net.IPEndPoint, NodeId), Stats> ();

        // Tunables (lightweight defaults)
        const double SampleWeight = 1.0;
        const double ErrorWeight = 2.0;
        static readonly TimeSpan HalfLife = TimeSpan.FromHours (6); // decay signal over time

        struct Stats
        {
            public double Score;
            public DateTime LastUpdateUtc;
            public int TotalSamples;
            public int TotalErrors;
        }

        static double DecayFactor (DateTime last)
        {
            var dt = DateTime.UtcNow - last;
            if (dt <= TimeSpan.Zero)
                return 1.0;
            // Exponential decay: factor = 0.5^(dt / HalfLife)
            return Math.Pow (0.5, dt.TotalSeconds / HalfLife.TotalSeconds);
        }

        (System.Net.IPEndPoint, NodeId) Key (Node n) => (n.EndPoint, n.Id);

        public void RecordYield (Node node, int newKeys, int total)
        {
            var k = Key (node);
            if (!_map.TryGetValue (k, out var s))
                s = new Stats { LastUpdateUtc = DateTime.UtcNow };
            else
                s.Score *= DecayFactor (s.LastUpdateUtc);

            // Tunables: favour novelty, give small weight to volume
            const double NewWeight = 1.0;
            const double VolumeWeight = 0.1; // duplicates/overlap still count a bit

            s.Score += (newKeys * NewWeight) + ((total - newKeys) * VolumeWeight);
            s.TotalSamples += total;
            s.LastUpdateUtc = DateTime.UtcNow;
            _map[k] = s;
        }

        public void RecordError (Node node, ErrorCode code)
        {
            var k = Key (node);
            if (!_map.TryGetValue (k, out var s)) {
                s = new Stats { LastUpdateUtc = DateTime.UtcNow };
            } else {
                s.Score *= DecayFactor (s.LastUpdateUtc);
            }
            // Penalise most errors the same; you can special-case if needed
            s.Score -= ErrorWeight;
            s.TotalErrors += 1;
            s.LastUpdateUtc = DateTime.UtcNow;
            _map[k] = s;
        }

        public double GetScore (Node node)
        {
            var k = Key (node);
            if (_map.TryGetValue (k, out var s)) {
                return s.Score * DecayFactor (s.LastUpdateUtc);
            }
            return 0.0; // unknown = neutral
        }
    }
}
