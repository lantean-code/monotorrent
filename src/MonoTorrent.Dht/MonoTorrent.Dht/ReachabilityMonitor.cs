using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;

namespace MonoTorrent.Dht
{
    sealed class ReachabilityMonitor
    {
        readonly HashSet<IPEndPoint> _bootstrap = new HashSet<IPEndPoint> ();
        readonly Dictionary<IPEndPoint, DateTime> _sent = new Dictionary<IPEndPoint, DateTime> ();
        readonly Dictionary<IPEndPoint, DateTime> _unsolicited = new Dictionary<IPEndPoint, DateTime> ();

        static readonly TimeSpan ReflexiveWindow = TimeSpan.FromMinutes (10);
        static readonly TimeSpan UnsolicitedExpiry = TimeSpan.FromMinutes (30);
        static readonly int UnsolicitedDistinctThreshold = 3;

        static readonly TimeSpan Warmup = TimeSpan.FromMinutes (5);
        static readonly TimeSpan Tick = TimeSpan.FromSeconds (30);

        DateTime _readyObservedAt;
        bool _started;

        public bool IsFirewalled { get; private set; } = true;

        public ISet<IPEndPoint> BootstrapNodes => _bootstrap;

        public void Start ()
        {
            if (_started) {
                return;
            }

            _started = true;
            _readyObservedAt = DateTime.UtcNow;
            SetFirewalled (true);

            DhtEngine.MainLoop.QueueTimeout (Tick, () => {
                Evaluate ();
                return true;
            });
        }

        public void Stop ()
        {
            _started = false;
        }

        public void MarkSent (IPEndPoint? endpoint)
        {
            if (endpoint == null) {
                return;
            }

            var now = DateTime.UtcNow;
            _sent[endpoint] = now;

            if (_sent.Count > 5000) {
                foreach (var kv in _sent.OrderBy (k => k.Value).Take (500).ToArray ()) {
                    _sent.Remove (kv.Key);
                }
            }

            if ((now.Ticks & 0x3F) == 0) {
                var cutoff = now - (ReflexiveWindow + TimeSpan.FromMinutes (5));
                foreach (var kv in _sent.Where (k => k.Value < cutoff).ToArray ()) {
                    _sent.Remove (kv.Key);
                }
            }
        }

        public void ObserveInboundQuery (IPEndPoint endpoint)
        {
            if (_bootstrap.Contains (endpoint)) {
                return;
            }

            if (_sent.TryGetValue (endpoint, out var lastSent) && DateTime.UtcNow - lastSent < ReflexiveWindow) {
                return;
            }

            _unsolicited[endpoint] = DateTime.UtcNow;

            foreach (var kv in _unsolicited.ToArray ()) {
                if (DateTime.UtcNow - kv.Value > UnsolicitedExpiry) {
                    _unsolicited.Remove (kv.Key);
                }
            }

            if (IsFirewalled && _unsolicited.Count >= UnsolicitedDistinctThreshold) {
                SetFirewalled (false);
            }
        }

        void Evaluate ()
        {
            if (!_started) {
                return;
            }

            if (_unsolicited.Count > 0) {
                SetFirewalled (false);
                return;
            }

            if (_sent.Count == 0) {
                return;
            }

            if (DateTime.UtcNow - _readyObservedAt >= Warmup) {
                SetFirewalled (true);
            }
        }

        void SetFirewalled (bool value)
        {
            if (IsFirewalled == value) {
                return;
            }

            IsFirewalled = value;
        }
    }
}
