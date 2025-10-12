using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace MonoTorrent.Dht
{
    internal sealed class SamplingState
    {
        private readonly byte[] _localNodeId;   // 20 bytes
        private readonly int _intervalSeconds;

        private byte[] _salt;                   // current interval salt
        private DateTime _expiresUtc;           // next rotation time (UTC)

        public SamplingState (NodeId localNodeId, int intervalSeconds = 600)
        {
            if (localNodeId is null)
                throw new ArgumentNullException (nameof (localNodeId));

            _localNodeId = localNodeId.AsMemory ().ToArray ();
            _intervalSeconds = intervalSeconds > 0 ? intervalSeconds : 600;

            _salt = NewSalt ();
            _expiresUtc = DateTime.UtcNow.AddSeconds (_intervalSeconds);
        }

        public int IntervalSeconds => _intervalSeconds;

        public byte[] GetSalt ()
        {
            MaybeRotate ();
            // defensive copy; callers shouldn’t mutate shared state
            var copy = new byte[_salt.Length];
            Buffer.BlockCopy (_salt, 0, copy, 0, copy.Length);
            return copy;
        }

        public Random CreateIntervalRandom ()
        {
            var salt = GetSalt ();
            var seed = DeriveSeed (_localNodeId, salt);
            return new Random (seed);
        }

        public Random CreateCryptoRandom ()
        {
            var b = new byte[4];
            using (var rng = RandomNumberGenerator.Create ()) {
                rng.GetBytes (b);
            }
            int seed = (b[0] << 24) | (b[1] << 16) | (b[2] << 8) | b[3];
            if (seed == int.MinValue)
                seed = 0;
            return new Random (seed < 0 ? -seed : seed);
        }

        // Called from MainLoop-only code paths
        private void MaybeRotate ()
        {
            var now = DateTime.UtcNow;
            if (now < _expiresUtc) {
                return;
            }

            _salt = NewSalt ();
            _expiresUtc = now.AddSeconds (_intervalSeconds);
        }

        private static byte[] NewSalt ()
        {
            var s = new byte[16];
            using (var rng = RandomNumberGenerator.Create ()) {
                rng.GetBytes (s);
            }
            return s;
        }

        // Cross-TFM friendly seed derivation (SHA1 + first 4 bytes)
        private static int DeriveSeed (byte[] localNodeId, byte[] salt)
        {
            var input = new byte[localNodeId.Length + salt.Length];
            Buffer.BlockCopy (localNodeId, 0, input, 0, localNodeId.Length);
            Buffer.BlockCopy (salt, 0, input, localNodeId.Length, salt.Length);

            byte[] hash;
            using (var sha1 = SHA1.Create ()) {
                hash = sha1.ComputeHash (input);
            }

            int seed = (hash[0] << 24) | (hash[1] << 16) | (hash[2] << 8) | hash[3];
            if (seed == int.MinValue)
                seed = 0;
            return seed < 0 ? -seed : seed;
        }
    }
}
