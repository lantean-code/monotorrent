//
// DhtCapabilities.cs
//
// Authors:
//   Alan McGovern alan.mcgovern@gmail.com
//
// Copyright (C) 2006 Alan McGovern
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

namespace MonoTorrent.Dht
{
    /// <summary>
    /// Specifies the capabilities that can be enabled for a Distributed Hash Table (DHT) engine. These flags control
    /// how the DHT engine participates in peer-to-peer operations, such as responding to queries, storing peer
    /// information, and serving sample data.
    /// </summary>
    /// <remarks>Use the individual flags to customize the behavior of the DHT engine according to application
    /// requirements. Multiple capabilities can be combined using bitwise operations. The <see cref="Default"/> value
    /// enables the standard set of features for typical peer-to-peer scenarios.</remarks>
    [Flags]
    public enum DhtCapabilities
    {
        /// <summary>
        /// Indicates that no special capabilities are enabled. This does not mean that the DHT engine is inactive.
        /// </summary>
        None = 0,

        /// <summary>
        /// Indicates that the system should respond to incoming DHT queries, ping, find_node, get_peers and announce_peer
        /// </summary>
        AcceptInboundQueries = 1 << 0, // handle ping/find_node/get_peers/announce_peer

        /// <summary>
        /// Indicates that announced peers should be stored in the list of infohashes.
        /// </summary>
        StoreAnnouncedPeers = 1 << 1, // persist (infohash -> peer) on announce_peer

        /// <summary>
        /// Indicates that announce peer messages should be emitted as found peers.
        /// </summary>
        /// <remarks>Use this flag to signal that the system should treat announce peer events as
        /// discoveries of new peers. This can be useful in scenarios where peer discovery is triggered by announce
        /// messages rather than responses from get_peer.</remarks>
        EmitAnnouncePeersAsFound = 1 << 2, // fire AnnouncePeer events

        /// <summary>
        /// Indicates that 'values' should be included in get_peers responses.
        /// </summary>
        /// <remarks>This can be switched off to participate in the network without divulging any infohashes.</remarks>
        ServePeerValues = 1 << 3, // include 'values' in get_peers replies

        /// <summary>
        /// Indicates that the sytem should respond to sample_infohashes requests.
        /// </summary>
        ServeSamples = 1 << 4, // serve sample_infohashes requests


        /// <summary>
        /// Represents the default set of options, combining AcceptInboundQueries, StoreAnnouncedPeers, ServePeerValues,
        /// and ServeSamples.
        /// </summary>
        /// <remarks>Use this value to enable the standard behavior for peer-to-peer operations, including
        /// accepting inbound queries, storing announced peers, serving peer values, and serving samples. This is
        /// typically suitable for most scenarios where full peer functionality is desired.</remarks>
        Default = AcceptInboundQueries | StoreAnnouncedPeers | ServePeerValues | ServeSamples
    }
}
