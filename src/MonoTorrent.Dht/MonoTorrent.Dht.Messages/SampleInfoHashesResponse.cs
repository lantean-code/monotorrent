//
// SampleInfoHashesResponse.cs
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


using MonoTorrent.BEncoding;

namespace MonoTorrent.Dht.Messages
{
    sealed class SampleInfoHashesResponse : ResponseMessage
    {
        static readonly BEncodedString SamplesKey = new BEncodedString ("samples");
        static readonly BEncodedString NumKey = new BEncodedString ("num");
        static readonly BEncodedString IntervalKey = new BEncodedString ("interval");
        static readonly BEncodedString NodesKey = new BEncodedString ("nodes");

        public BEncodedString? Samples {
            get => (BEncodedString?) Parameters.GetValueOrDefault (SamplesKey);
            set {
                if (value is null)
                    Parameters.Remove (SamplesKey);
                else
                    Parameters[SamplesKey] = value;
            }
        }

        public BEncodedNumber? Num {
            get => (BEncodedNumber?) Parameters.GetValueOrDefault (NumKey);
            set {
                if (value is null)
                    Parameters.Remove (NumKey);
                else
                    Parameters[NumKey] = value;
            }
        }

        public BEncodedNumber? Interval {
            get => (BEncodedNumber?) Parameters.GetValueOrDefault (IntervalKey);
            set {
                if (value is null)
                    Parameters.Remove (IntervalKey);
                else
                    Parameters[IntervalKey] = value;
            }
        }

        public BEncodedString? Nodes {
            get => (BEncodedString?) Parameters.GetValueOrDefault (NodesKey);
            set {
                if (value is null)
                    Parameters.Remove (NodesKey);
                else
                    Parameters[NodesKey] = value;
            }
        }

        public SampleInfoHashesResponse (NodeId id, BEncodedValue transactionId)
            : base (id, transactionId)
        {

        }

        public SampleInfoHashesResponse (BEncodedDictionary d)
            : base (d)
        {

        }
    }
}
