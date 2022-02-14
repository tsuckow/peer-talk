using Common.Logging;
using Ipfs;
using PeerTalk.Protocols;
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
        public PeerList(Peer localPeer)
        {
            LocalPeer = localPeer;
            protocols = new List<IPeerProtocol>
            {
                new Multistream1(),
                new SecureCommunication.Noise.Noise(this),
                new SecureCommunication.Secio1(),
                new Identify1(),
                new Mplex67()
            };
        }

        private readonly Peer LocalPeer;

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
        ///   Raised when a new peer is discovered for the first time.
        /// </summary>
        public event EventHandler<Peer> PeerDiscovered;

        /// <summary>
        ///   Raised when a peer should no longer be used.
        /// </summary>
        /// <remarks>
        ///   This event indicates that the peer has been removed
        ///   and should no longer
        ///   be used.
        /// </remarks>
        public event EventHandler<Peer> PeerRemoved;

        /// <summary>
        ///   Returns a peer instance for a given multihash, if it isn't a known peer it is registered
        ///   
        ///   Resolving the local peer (ourselves) is permitted
        /// </summary>
        public Peer ResolvePeer(MultiHash id)
        {
            if (id == LocalPeer.Id)
            {
                return LocalPeer;
            }
            else
            {
                RegisterPeer(id, out Peer peer);
                return peer;
            }
        }

        /// <summary>
        ///  Returns a peer instance for a given multihash, if it isn't a known peer it is registered
        ///   
        ///  Resolving the local peer (ourselves) is permitted
        ///  
        ///  If the id is blocked, returns null;
        /// </summary>
        public Peer ResolvePeerOrNull(MultiHash id)
        {
            try
            {
                return ResolvePeer(id);
            }
            catch(Exception)
            {
                return null;
            }
        }

        /// <summary>
        ///   Locates or creates a peer, merging addresses
        ///   
        ///   Registering the local peer (ourselves) is not permitted
        /// </summary>
        public bool RegisterPeer(MultiHash id, out Peer peer, IEnumerable<MultiAddress> addresses = null)
        {
            if (id == null)
            {
                throw new ArgumentNullException(nameof(id));
            }
            if (id == LocalPeer.Id)
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
                    var newpeer = new Peer { Id = id, Addresses = addresses ?? Enumerable.Empty<MultiAddress>() };
                    PeerDiscovered.Invoke(this, newpeer);
                    return newpeer;
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
            if(found != null)
            {
                PeerRemoved.Invoke(this, found);
            }
        }

        /// <summary>
        ///   Removes all peers
        /// </summary>
        public void Clear()
        {
            otherPeers.Clear();
        }

        /// <summary>
        ///  The supported protocols.
        /// </summary>
        /// <remarks>
        ///   Use sychronized access, e.g. <code>lock (protocols) { ... }</code>.
        /// </remarks>
        List<IPeerProtocol> protocols;

        /// <summary>
        ///   Gets the registered encryption protocols
        /// </summary>
        public IEnumerable<IEncryptionProtocol> EncryptionProtocols
        {
            get
            {
                lock (protocols)
                {
                    return protocols.OfType<IEncryptionProtocol>().ToArray();
                }
            }
        }

        /// <summary>
        ///   Gets the registered Identity protocol
        /// </summary>
        public Identify1 IdentifyProtocol { get
            {
                lock (protocols)
                {
                    return protocols.OfType<Identify1>().First();
                }
            }
        }

        /// <summary>
        ///   Add a protocol that is supported by the swarm.
        /// </summary>
        /// <param name="protocol">
        ///   The protocol to add.
        /// </param>
        public void AddProtocol(IPeerProtocol protocol)
        {
            lock (protocols)
            {
                protocols.Add(protocol);
            }
        }

        /// <summary>
        ///   Remove a protocol from the swarm.
        /// </summary>
        /// <param name="protocol">
        ///   The protocol to remove.
        /// </param>
        public void RemoveProtocol(IPeerProtocol protocol)
        {
            lock (protocols)
            {
                protocols.Remove(protocol);
            }
        }

        /// <summary>
        ///   Adds Protocols to a connection
        /// </summary>
        public void MountProtocols(PeerConnection connection)
        {
            lock (protocols)
            {
                connection.AddProtocols(protocols);
            }
        }
    }
}
