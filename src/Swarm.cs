using Ipfs;
using PeerTalk.Transports;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using System.Text;
using System.Collections.Concurrent;
using System.Threading;
using Common.Logging;
using System.IO;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using PeerTalk.Protocols;
using PeerTalk.Cryptography;
using Ipfs.CoreApi;
using Nito.AsyncEx;

namespace PeerTalk
{
    /// <summary>
    ///   Manages communication with other peers.
    /// </summary>
    public class Swarm
    {
        static readonly ILog log = LogManager.GetLogger(typeof(Swarm));

        /// <summary>
        ///   The time to wait for a low level connection to be established.
        /// </summary>
        /// <value>
        ///   Defaults to 30 seconds.
        /// </value>
        public TimeSpan TransportConnectionTimeout = TimeSpan.FromSeconds(30);   

        /// <summary>
        ///   Added to connection protocols when needed.
        /// </summary>
        readonly Plaintext1 plaintext1 = new Plaintext1();

        Peer localPeer;

        /// <summary>
        ///   Raised when a connection to another peer is established.
        /// </summary>
        public event EventHandler<PeerConnection> ConnectionEstablished;

        void OnConnectionEstablished(object sender, PeerConnection connection)
        {
            ConnectionEstablished.Invoke(sender, connection);
        }

        /// <summary>
        ///   Raised when a new peer is discovered for the first time.
        /// </summary>
        public event EventHandler<Peer> PeerDiscovered;

        /// <summary>
        ///   Raised when a peer's connection is closed.
        /// </summary>
        public event EventHandler<Peer> PeerDisconnected;

        /// <summary>
        ///   Raised when a peer cannot be connected to.
        /// </summary>
        public event EventHandler<Peer> PeerNotReachable;

        void OnPeerNotReachable(object sender, Peer peer)
        {
            PeerNotReachable.Invoke(sender, peer);
        }

        /// <summary>
        ///  The local peer.
        /// </summary>
        /// <value>
        ///   The local peer must have an <see cref="Peer.Id"/> and
        ///   <see cref="Peer.PublicKey"/>.
        /// </value>
        public Peer LocalPeer
        {
            get { return localPeer; }
            set
            {
                if (value == null)
                    throw new ArgumentNullException();
                if (value.Id == null)
                    throw new ArgumentNullException("peer.Id");
                if (value.PublicKey == null)
                    throw new ArgumentNullException("peer.PublicKey");
                if (!value.IsValid())
                    throw new ArgumentException("Invalid peer.");
                localPeer = value;
            }
        }

        /// <summary>
        ///   List of other known peers.
        /// </summary>
        PeerList otherPeers;

        /// <summary>
        ///   Use to find addresses of a peer.
        /// </summary>
        private readonly IPeerRouting Router;

        private readonly Switchboard Switchboard;

        /// <summary>
        ///   Get the sequence of all known peer addresses.
        /// </summary>
        /// <value>
        ///   Contains any peer address that has been
        ///   <see cref="RegisterPeerAddress">discovered</see>.
        /// </value>
        /// <seealso cref="RegisterPeerAddress"/>
        public IEnumerable<MultiAddress> KnownPeerAddresses
        {
            get
            {
                return otherPeers
                    .Peers
                    .SelectMany(p => p.Addresses);
            }
        }

        /// <summary>
        ///   Get the sequence of all known peers.
        /// </summary>
        /// <value>
        ///   Contains any peer that has been
        ///   <see cref="RegisterPeerAddress">discovered</see>.
        /// </value>
        /// <seealso cref="RegisterPeerAddress"/>
        public IEnumerable<Peer> KnownPeers
        {
            get
            {
                return otherPeers.Peers;
            }
        }

        /// <summary>
        ///   Instantiate the Swarm
        /// </summary>
        public Swarm(Peer localPeer, PeerList peerList, Switchboard switchboard, IPeerRouting router)
        {
            if (localPeer == null)
                throw new ArgumentNullException();
            if (localPeer.Id == null)
                throw new ArgumentNullException("peer.Id");
            if (localPeer.PublicKey == null)
                throw new ArgumentNullException("peer.PublicKey");
            if (!localPeer.IsValid())
                throw new ArgumentException("Invalid peer.");
            LocalPeer = localPeer;
            otherPeers = peerList;
            Switchboard = switchboard;
            Router = router;

            Switchboard.ConnectionEstablished += OnConnectionEstablished;
            Switchboard.PeerNotReachable += OnPeerNotReachable;

            peerList.PeerDiscovered += (o, peer) => PeerDiscovered.Invoke(o, peer);
        }

        /// <summary>
        ///   Register that a peer's address has been discovered.
        /// </summary>
        /// <param name="address">
        ///   An address to the peer. It must end with the peer ID.
        /// </param>
        /// <returns>
        ///   The <see cref="Peer"/> that is registered.
        /// </returns>
        /// <exception cref="Exception">
        ///   The <see cref="PeerList.DenyList"/> or <see cref="PeerList.AllowList"/> policies forbid it.
        ///   Or the "p2p/ipfs" protocol name is missing.
        /// </exception>
        /// <remarks>
        ///   If the <paramref name="address"/> is not already known, then it is
        ///   added to the <see cref="KnownPeerAddresses"/>.
        /// </remarks>
        /// <seealso cref="RegisterPeer(MultiHash, IEnumerable{MultiAddress})"/>
        public Peer RegisterPeerAddress(MultiAddress address)
        {
            return RegisterPeer(address.PeerId, new[] { address });
        }

        /// <summary>
        ///   Register that a peer has been discovered.
        /// </summary>
        /// <param name="id">
        ///   The newly discovered peer.
        /// </param>
        /// <param name="addresses">
        ///   Any known addresses for the peer.
        /// </param>
        /// <returns>
        ///   The registered peer.
        /// </returns>
        /// <remarks>
        ///   If the peer already exists, then the existing peer is updated with supplied
        ///   information and is then returned.  Otherwise, the <paramref name="id"/>
        ///   is added to known peers and is returned.
        ///   <para>
        ///   If the peer already exists, then a union of the existing and new addresses
        ///   is used.
        ///   </para>
        ///   <para>
        ///   If peer does not already exist, then the <see cref="PeerDiscovered"/> event
        ///   is raised.
        ///   </para>
        /// </remarks>
        /// <exception cref="Exception">
        ///   The <see cref="PeerList.DenyList"/> or <see cref="PeerList.AllowList"/> policies forbid it.
        /// </exception>
        public Peer RegisterPeer(MultiHash id, IEnumerable<MultiAddress> addresses = null)
        {
            var isNew = otherPeers.RegisterPeer(id, out Peer peer, addresses);

            if (isNew)
            {
                if (log.IsDebugEnabled)
                {
                    log.Debug($"New peer registerd {peer}");
                }
            }

            return peer;
        }

        /// <summary>
        ///   Deregister a peer.
        /// </summary>
        /// <param name="id">
        ///   The peer to remove..
        /// </param>
        public bool DeregisterPeer(MultiHash id)
        {
            if (id == null)
            {
                throw new ArgumentNullException(nameof(id));
            }

            otherPeers.RemovePeer(id, out Peer found);
            return found != null;
        }

        /// <summary>
        ///   Determines if a connection is being made to the peer.
        /// </summary>
        /// <param name="peer">
        ///   A <see cref="Peer"/>.
        /// </param>
        /// <returns>
        ///   <b>true</b> is the <paramref name="peer"/> has a pending connection.
        /// </returns>
        public bool HasPendingConnection(Peer peer)
        {
            return Switchboard.HasPendingConnection(peer);
        }

        /// <summary>
        ///   The addresses that cannot be used.
        /// </summary>
        public MultiAddressDenyList DenyList { get => otherPeers.DenyList; }

        /// <summary>
        ///   The addresses that can be used.
        /// </summary>
        public MultiAddressAllowList AllowList { get => otherPeers.AllowList; }

        void OnPeerDisconnected(object sender, MultiHash peerId)
        {
            PeerDisconnected?.Invoke(this, RegisterPeer(peerId));
        }


        /// <summary>
        ///   Connect to a peer using the specified <see cref="MultiAddress"/>.
        /// </summary>
        /// <param name="address">
        ///   An ipfs <see cref="MultiAddress"/>, such as
        ///  <c>/ip4/104.131.131.82/tcp/4001/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ</c>.
        /// </param>
        /// <param name="cancel">
        ///   Is used to stop the task.  When cancelled, the <see cref="TaskCanceledException"/> is raised.
        /// </param>
        /// <returns>
        ///   A task that represents the asynchronous operation. The task's result
        ///   is the <see cref="PeerConnection"/>.
        /// </returns>
        /// <remarks>
        ///   If already connected to the peer and is active on any address, then
        ///   the existing connection is returned.
        /// </remarks>
        public async Task<PeerConnection> ConnectAsync(MultiAddress address, CancellationToken cancel = default(CancellationToken))
        {
            var peer = RegisterPeerAddress(address);
            return await ConnectAsync(peer, cancel).ConfigureAwait(false);
        }

        /// <summary>
        ///   Connect to a peer.
        /// </summary>
        /// <param name="peer">
        ///  A peer to connect to.
        /// </param>
        /// <param name="cancel">
        ///   Is used to stop the task.  When cancelled, the <see cref="TaskCanceledException"/> is raised.
        /// </param>
        /// <returns>
        ///   A task that represents the asynchronous operation. The task's result
        ///   is the <see cref="PeerConnection"/>.
        /// </returns>
        /// <remarks>
        ///   If already connected to the peer and is active on any address, then
        ///   the existing connection is returned.
        /// </remarks>
        public Task<PeerConnection> ConnectAsync(Peer peer, CancellationToken cancel = default(CancellationToken))
        {
            return Switchboard.ConnectAsync(peer, cancel);
        }

            /// <summary>
            ///   Create a stream to the peer that talks the specified protocol.
            /// </summary>
            /// <param name="peer">
            ///   The remote peer.
            /// </param>
            /// <param name="protocol">
            ///   The protocol name, such as "/foo/0.42.0".
            /// </param>
            /// <param name="cancel">
            ///   Is used to stop the task.  When cancelled, the <see cref="TaskCanceledException"/> is raised.
            /// </param>
            /// <returns>
            ///   A task that represents the asynchronous operation. The task's result
            ///   is the new <see cref="Stream"/> to the <paramref name="peer"/>.
            /// </returns>
            /// <remarks>
            ///   <para>
            ///   When finished, the caller must <see cref="Stream.Dispose()"/> the
            ///   new stream.
            ///   </para>
            /// </remarks>
            public Task<Stream> DialAsync(Peer peer, string protocol, CancellationToken cancel = default(CancellationToken))
        {
            return Switchboard.DialAsync(peer, protocol, cancel);

        }

        /// <summary>
        ///   Disconnect from a peer.
        /// </summary>
        /// <param name="address">
        ///   An ipfs <see cref="MultiAddress"/>, such as
        ///  <c>/ip4/104.131.131.82/tcp/4001/ipfs/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ</c>.
        /// </param>
        /// <param name="cancel">
        ///   Is used to stop the task.  When cancelled, the <see cref="TaskCanceledException"/> is raised.
        /// </param>
        /// <returns>
        ///   A task that represents the asynchronous operation.
        /// </returns>
        /// <remarks>
        ///   If the peer is not conected, then nothing happens.
        /// </remarks>
        public Task DisconnectAsync(MultiAddress address, CancellationToken cancel = default(CancellationToken))
        {
            return Switchboard.DisconnectAsync(address, cancel);
        }

        /// <summary>
        /// Number of current connections
        /// </summary>
        /// <returns></returns>
        public int ConnectionCount()
        {
            return Switchboard.ConnectionCount();
        }

        /// <summary>
        ///   Add a protocol that is supported by the swarm.
        /// </summary>
        /// <param name="protocol">
        ///   The protocol to add.
        /// </param>
        public void AddProtocol(IPeerProtocol protocol)
        {
            otherPeers.AddProtocol(protocol);
        }

        /// <summary>
        ///   Remove a protocol from the swarm.
        /// </summary>
        /// <param name="protocol">
        ///   The protocol to remove.
        /// </param>
        public void RemoveProtocol(IPeerProtocol protocol)
        {
            otherPeers.RemoveProtocol(protocol);
        }
    }
}
