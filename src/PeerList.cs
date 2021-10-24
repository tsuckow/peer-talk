using Common.Logging;
using Ipfs;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PeerTalk
{
    /// <summary>
    ///   Maintains the list of known peers
    /// </summary>
    public class PeerList : IPolicy<MultiAddress>, IPolicy<Peer>
    {
        static ILog log = LogManager.GetLogger(typeof(PeerList));

        /// <summary>
        ///   Instantiate a PeerList
        /// </summary>
        public PeerList(MultiHash selfId)
        {
            SelfId = selfId;
        }

        private readonly MultiHash SelfId;

        /// <summary>
        ///   The addresses that cannot be used.
        /// </summary>
        public MultiAddressDenyList DenyList { get; set; } = new MultiAddressDenyList();

        /// <summary>
        ///   The addresses that can be used.
        /// </summary>
        public MultiAddressAllowList AllowList { get; set; } = new MultiAddressAllowList();

        /// <summary>
        ///   Other nodes. Key is the base58 hash of the peer ID.
        /// </summary>
        private readonly ConcurrentDictionary<string, Peer> otherPeers = new ConcurrentDictionary<string, Peer>();

        /// <summary>
        ///   All the known peers
        /// </summary>
        public IEnumerable<Peer> Peers { get => otherPeers.Values; }

        /// <summary>
        ///   Locates or creates a peer, merging addresses
        /// </summary>
        public bool RegisterPeer(MultiHash id, out Peer peer, IEnumerable<MultiAddress> addresses = null)
        {
            if (id == null)
            {
                throw new ArgumentNullException(nameof(id));
            }
            if (id == SelfId)
            {
                throw new ArgumentException("Cannot register self.");
            }
            if (addresses != null && !IsAllowed(addresses))
            {
                throw new Exception($"Communication with '{id}' is not allowed.");
            }

            var isNew = false;
            peer = otherPeers.AddOrUpdate(id.ToBase58(),
                (_) =>
                {
                    isNew = true;
                    return new Peer { Id = id, Addresses = addresses ?? Enumerable.Empty<MultiAddress>() };
                },
                (_, existing) =>
                {
                   existing.Addresses = existing
                            .Addresses
                            .Union(addresses ?? Enumerable.Empty<MultiAddress>())
                            .ToList();
                    
                    return existing;
                });

            return isNew;
        }


        /// <inheritdoc />
        public bool IsAllowed(MultiAddress target)
        {
            return DenyList.IsAllowed(target)
                && AllowList.IsAllowed(target);
        }

        /// <inheritdoc />
        public bool IsAllowed(Peer peer)
        {
            return peer.Addresses.All(a => IsAllowed(a));
        }

        /// <inheritdoc />
        public bool IsAllowed(IEnumerable<MultiAddress> addresses)
        {
            return addresses.All(a => IsAllowed(a));
        }

        /// <summary>
        ///   Removes a peer from accounting
        /// </summary>
        public void RemovePeer(MultiHash id, out Peer found)
        {
            otherPeers.TryRemove(id.ToBase58(), out found);
        }

        /// <summary>
        ///   Removes all peers
        /// </summary>
        public void Clear()
        {
            otherPeers.Clear();
        }
    }
}
