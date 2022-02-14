using Common.Logging;
using Ipfs;
using Nito.AsyncEx;
using PeerTalk.Cryptography;
using PeerTalk.Protocols;
using PeerTalk.Transports;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PeerTalk
{
    /// <summary>
    /// Handles creating / recieving a connection with a peer given one or more addresses.
    /// 
    /// This CANNOT look up a peer by ID, if you need that use Swarm
    /// </summary>
    public class Switchboard : IService
    {
        static readonly ILog log = LogManager.GetLogger(typeof(Switchboard));

        /// <summary>
        ///   The time to wait for a low level connection to be established.
        /// </summary>
        /// <value>
        ///   Defaults to 30 seconds.
        /// </value>
        public TimeSpan TransportConnectionTimeout = TimeSpan.FromSeconds(30);

        /// <summary>
        ///  The local peer.
        /// </summary>
        /// <value>
        ///   The local peer must have an <see cref="Peer.Id"/> and
        ///   <see cref="Peer.PublicKey"/>.
        /// </value>
        private readonly Peer LocalPeer;

        /// <summary>
        ///   The private key of the local peer.
        /// </summary>
        /// <value>
        ///   Used to prove the identity of the <see cref="LocalPeer"/>.
        /// </value>
        private readonly Key LocalPeerKey;

        /// <summary>
        ///   List of other known peers.
        /// </summary>
        private readonly PeerList OtherPeers;

        /// <summary>
        ///   Provides access to a private network of peers.
        /// </summary>
        private readonly INetworkProtector NetworkProtector;

        /// <summary>
        ///   Manages the swarm's peer connections.
        /// </summary>
        public readonly ConnectionManager Manager = new ConnectionManager();

        /// <summary>
        ///   Used to cancel any task when the swarm is stopped.
        /// </summary>
        private CancellationTokenSource swarmCancellation;

        /// <summary>
        ///   Cancellation tokens for the listeners.
        /// </summary>
        private readonly ConcurrentDictionary<MultiAddress, CancellationTokenSource> listeners = new ConcurrentDictionary<MultiAddress, CancellationTokenSource>();

        /// <summary>
        ///  Outstanding connection tasks initiated by the local peer.
        /// </summary>
        private readonly ConcurrentDictionary<Peer, AsyncLazy<PeerConnection>> pendingConnections = new ConcurrentDictionary<Peer, AsyncLazy<PeerConnection>>();

        /// <summary>
        ///  Outstanding connection tasks initiated by a remote peer.
        /// </summary>
        private readonly ConcurrentDictionary<MultiAddress, object> pendingRemoteConnections = new ConcurrentDictionary<MultiAddress, object>();

        /// <summary>
        ///   Determines if the swarm has been started.
        /// </summary>
        /// <value>
        ///   <b>true</b> if the swarm has started; otherwise, <b>false</b>.
        /// </value>
        /// <seealso cref="StartAsync"/>
        /// <seealso cref="StopAsync"/>
        private bool IsRunning = false;

        /// <summary>
        ///   Raised when a peer's connection is closed.
        /// </summary>
        public event EventHandler<Peer> PeerDisconnected;

        /// <summary>
        ///   Raised when a connection to another peer is established.
        /// </summary>
        public event EventHandler<PeerConnection> ConnectionEstablished;

        /// <summary>
        ///   Raised when a peer cannot be connected to.
        /// </summary>
        public event EventHandler<Peer> PeerNotReachable;

        /// <summary>
        ///   Creates a swichboard for connecting to and recieving connections from other peers.
        /// </summary>
        /// <param name="localPeer"></param>
        /// <param name="localPeerKey"></param>
        /// <param name="otherPeers"></param>
        /// <param name="networkProtector"></param>
        public Switchboard(Peer localPeer, Key localPeerKey, PeerList otherPeers, INetworkProtector networkProtector = null)
        {
            LocalPeer = localPeer;
            LocalPeerKey = localPeerKey;
            NetworkProtector = networkProtector;
            OtherPeers = otherPeers;
        }

        void OnPeerDisconnected(object sender, MultiHash peerId)
        {
            PeerDisconnected?.Invoke(this, OtherPeers.ResolvePeer(peerId));
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
            Manager.Remove(address.PeerId);
            return Task.CompletedTask;
        }

        /// <summary>
        /// Number of current connections
        /// </summary>
        /// <returns></returns>
        public int ConnectionCount()
        {
            return Manager.Connections.Count();
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
            return pendingConnections.TryGetValue(peer, out AsyncLazy<PeerConnection> _);
        }

        /// <inheritdoc />
        public Task StartAsync()
        {
            if (LocalPeer == null)
            {
                throw new NotSupportedException("The LocalPeer is not defined.");
            }

            Manager.PeerDisconnected += OnPeerDisconnected;
            IsRunning = true;
            swarmCancellation = new CancellationTokenSource();
            log.Debug("Started Switchboard");

            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public async Task StopAsync()
        {
            IsRunning = false;
            swarmCancellation?.Cancel(true);

            log.Debug($"Stopping Switchboard");

            // Stop the listeners.
            while (listeners.Count > 0)
            {
                await StopListeningAsync(listeners.Keys.First()).ConfigureAwait(false);
            }

            // Disconnect from remote peers.
            Manager.Clear();
            Manager.PeerDisconnected -= OnPeerDisconnected;

            listeners.Clear();
            pendingConnections.Clear();
            pendingRemoteConnections.Clear();

            log.Debug($"Stopped Switchboard");
        }


        /// <summary>
        ///   Start listening on the specified <see cref="MultiAddress"/>.
        /// </summary>
        /// <param name="address">
        ///   Typically "/ip4/0.0.0.0/tcp/4001" or "/ip6/::/tcp/4001".
        /// </param>
        /// <returns>
        ///   A task that represents the asynchronous operation.  The task's result
        ///   is a <see cref="MultiAddress"/> than can be used by another peer
        ///   to connect to tis peer.
        /// </returns>
        /// <exception cref="Exception">
        ///   Already listening on <paramref name="address"/>.
        /// </exception>
        /// <exception cref="ArgumentException">
        ///   <paramref name="address"/> is missing a transport protocol (such as tcp or udp).
        /// </exception>
        /// <remarks>
        ///   Allows other peers to connect to us
        ///   <para>
        ///   The <see cref="Peer.Addresses"/> of the <see cref="LocalPeer"/> are updated.  If the <paramref name="address"/> refers to
        ///   any IP address ("/ip4/0.0.0.0" or "/ip6/::") then all network interfaces addresses
        ///   are added.  If the port is zero (as in "/ip6/::/tcp/0"), then the peer addresses contains the actual port number
        ///   that was assigned.
        ///   </para>
        /// </remarks>
        public Task<MultiAddress> StartListeningAsync(MultiAddress address)
        {
            var cancel = new CancellationTokenSource();

            if (!listeners.TryAdd(address, cancel))
            {
                throw new Exception($"Already listening on '{address}'.");
            }

            // Start a listener for the transport
            var didSomething = false;
            foreach (var protocol in address.Protocols)
            {
                if (TransportRegistry.Transports.TryGetValue(protocol.Name, out Func<IPeerTransport> transport))
                {
                    address = transport().Listen(address, OnRemoteConnect, cancel.Token);
                    listeners.TryAdd(address, cancel);
                    didSomething = true;
                    break;
                }
            }
            if (!didSomething)
            {
                throw new ArgumentException($"Missing a transport protocol name '{address}'.", "address");
            }

            var result = new MultiAddress($"{address}/ipfs/{LocalPeer.Id}");

            // Get the actual IP address(es).
            IEnumerable<MultiAddress> addresses = new List<MultiAddress>();
            var ips = NetworkInterface.GetAllNetworkInterfaces()
                // It appears that the loopback adapter is not UP on Ubuntu 14.04.5 LTS
                .Where(nic => nic.OperationalStatus == OperationalStatus.Up
                    || nic.NetworkInterfaceType == NetworkInterfaceType.Loopback)
                .SelectMany(nic => nic.GetIPProperties().UnicastAddresses);
            if (result.ToString().StartsWith("/ip4/0.0.0.0/"))
            {
                addresses = ips
                    .Where(ip => ip.Address.AddressFamily == AddressFamily.InterNetwork)
                    .Select(ip =>
                    {
                        return new MultiAddress(result.ToString().Replace("0.0.0.0", ip.Address.ToString()));
                    })
                    .ToArray();
            }
            else if (result.ToString().StartsWith("/ip6/::/"))
            {
                addresses = ips
                    .Where(ip => ip.Address.AddressFamily == AddressFamily.InterNetworkV6)
                    .Select(ip =>
                    {
                        return new MultiAddress(result.ToString().Replace("::", ip.Address.ToString()));
                    })
                    .ToArray();
            }
            else
            {
                addresses = new MultiAddress[] { result };
            }
            if (addresses.Count() == 0)
            {
                var msg = "Cannot determine address(es) for " + result;
                foreach (var ip in ips)
                {
                    msg += " nic-ip: " + ip.Address.ToString();
                }
                cancel.Cancel();
                throw new Exception(msg);
            }

            // Add actual addresses to listeners and local peer addresses.
            foreach (var a in addresses)
            {
                log.Debug($"Listening on {a}");
                listeners.TryAdd(a, cancel);
            }
            LocalPeer.Addresses = LocalPeer
                .Addresses
                .Union(addresses)
                .ToArray();

            return Task.FromResult(addresses.First());
        }


        /// <summary>
        ///   Stop listening on the specified <see cref="MultiAddress"/>.
        /// </summary>
        /// <param name="address"></param>
        /// <returns>
        ///   A task that represents the asynchronous operation.
        /// </returns>
        public async Task StopListeningAsync(MultiAddress address)
        {
            if (!listeners.TryRemove(address, out CancellationTokenSource listener))
            {
                return;
            }

            try
            {
                if (!listener.IsCancellationRequested)
                {
                    listener.Cancel(false);
                }

                // Remove any local peer address that depend on the cancellation token.
                var others = listeners
                    .Where(l => l.Value == listener)
                    .Select(l => l.Key)
                    .ToArray();

                LocalPeer.Addresses = LocalPeer.Addresses
                    .Where(a => a != address)
                    .Where(a => !others.Contains(a))
                    .ToArray();

                foreach (var other in others)
                {
                    listeners.TryRemove(other, out CancellationTokenSource _);
                }

                // Give some time away, so that cancel can run
                // TODO: Would be nice to make this deterministic.
                await Task.Delay(TimeSpan.FromMilliseconds(100)).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                log.Error("stop listening failed", e);
            }
        }


#pragma warning disable VSTHRD100 // Avoid async void methods
        /// <summary>
        ///   Called when a remote peer is connecting to the local peer.
        /// </summary>
        /// <param name="stream">
        ///   The stream to the remote peer.
        /// </param>
        /// <param name="local">
        ///   The local peer's address.
        /// </param>
        /// <param name="remote">
        ///   The remote peer's address.
        /// </param>
        /// <remarks>
        ///   Establishes the protocols of the connection.  Any exception is simply
        ///   logged as warning.
        /// </remarks>
        async void OnRemoteConnect(Stream stream, MultiAddress local, MultiAddress remote)
#pragma warning restore VSTHRD100 // Avoid async void methods
        {
            if (!IsRunning)
            {
                try
                {
                    stream.Dispose();
                }
                catch (Exception)
                {
                    // eat it.
                }
                return;
            }

            // If the remote is already trying to establish a connection, then we
            // can just refuse this one.
            if (!pendingRemoteConnections.TryAdd(remote, null))
            {
                log.Info($"Duplicate remote connection from {remote}");
                try
                {
                    stream.Dispose();
                }
                catch (Exception)
                {
                    // eat it.
                }
                return;
            }

            try
            {
                if (log.IsDebugEnabled)
                {
                    log.Debug($"remote connect from {remote}");
                }

                // TODO: Check the policies

                var connection = new PeerConnection
                {
                    IsIncoming = true,
                    LocalPeer = LocalPeer,
                    LocalAddress = local,
                    LocalPeerKey = LocalPeerKey,
                    RemoteAddress = remote,
                    Stream = stream
                };

                // Are we communicating to a private network?
                if (NetworkProtector != null)
                {
                    connection.Stream = await NetworkProtector.ProtectAsync(connection).ConfigureAwait(false);
                }

                // Mount the protocols.
                OtherPeers.MountProtocols(connection);

                // Start the handshake
                _ = connection.ReadMessagesAsync(swarmCancellation.Token);

                // Wait for security to be established.
                await connection.SecurityEstablished.Task.ConfigureAwait(false);
                // TODO: Maybe connection.LocalPeerKey = null;

                // Wait for the handshake to complete.
                _ = await connection.MuxerEstablished.Task.ConfigureAwait(false);

                // Need details on the remote peer.
                Identify1 identify = OtherPeers.IdentifyProtocol;
                connection.RemotePeer = await identify.GetRemotePeerAsync(connection, swarmCancellation.Token).ConfigureAwait(false);

                //FIXME: This whole remotepeer assignment is sketch AF.
                Debugger.Break();
                //OtherPeers.RegisterPeer(connection.RemotePeer.Id, out connection.RemotePeer);
                connection.RemoteAddress = new MultiAddress($"{remote}/ipfs/{connection.RemotePeer.Id}");
                var actual = Manager.Add(connection);
                if (actual == connection)
                {
                    ConnectionEstablished?.Invoke(this, connection);
                }
            }
            catch (Exception e)
            {
                if (log.IsDebugEnabled)
                {
                    log.Debug($"remote connect from {remote} failed: {e.Message}");
                }
                try
                {
                    stream.Dispose();
                }
                catch (Exception)
                {
                    // eat it.
                }
            }
            finally
            {
                pendingRemoteConnections.TryRemove(remote, out object _);
            }
        }

        /// <summary>
        ///   Create a stream to the peer that talks the specified protocol.
        /// </summary>
        /// <param name="peer">
        ///   The remote peer.
        /// </param>
        /// <param name="protocol">
        ///   The protocol to negotiate.
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
        public async Task<ProtocolStream<Protocol>> DialAsync<Protocol>(Peer peer, Protocol protocol, CancellationToken cancel = default(CancellationToken)) where Protocol : IPeerProtocol
        {
            return await DialAsync(peer, new[] { protocol }, cancel);
        }

        /// <summary>
        ///   Create a stream to the peer that talks the specified protocol.
        /// </summary>
        /// <param name="peer">
        ///   The remote peer.
        /// </param>
        /// <param name="protocols">
        ///   The protocols to negotiate.
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
        public async Task<ProtocolStream<Protocol>> DialAsync<Protocol>(Peer peer, IEnumerable<Protocol> protocols, CancellationToken cancel = default(CancellationToken)) where Protocol : IPeerProtocol
        {
            peer = OtherPeers.ResolvePeer(peer.Id);

            // Get a connection and then a muxer to the peer.
            var connection = await ConnectAsync(peer, cancel).ConfigureAwait(false);
            var muxer = await connection.MuxerEstablished.Task.ConfigureAwait(false);

            // Create a new stream for the peer protocol.
            var stream = await muxer.CreateStreamAsync("Dialing").ConfigureAwait(false);
            try
            {
                var protocol = await new Multistream1().NegotiateProtocolAsync(connection, stream, protocols, cancel);
                stream.Name = protocol.ToString();
                return ProtocolStream.Wrap(stream, protocol);
            }
            catch (Exception)
            {
                stream.Dispose();
                throw;
            }
        }


        /// <summary>
        ///   Establish a duplex stream between the local and remote peer.
        /// </summary>
        /// <param name="remote"></param>
        /// <param name="addrs"></param>
        /// <param name="cancel"></param>
        /// <returns></returns>
        async Task<PeerConnection> DialAsync(Peer remote, IEnumerable<MultiAddress> addrs, CancellationToken cancel)
        {
            log.Debug($"Dialing {remote}");

            if (remote == LocalPeer)
            {
                throw new Exception("Cannot dial self.");
            }

            // Get the addresses we can use to dial the remote.  Filter
            // out any addresses (ip and port) we are listening on.
            var blackList = listeners.Keys
                .Select(a => a.WithoutPeerId())
                .ToArray();
            var possibleAddresses = (await Task.WhenAll(addrs.Select(a => a.ResolveAsync(cancel))).ConfigureAwait(false))
                .SelectMany(a => a)
                .Where(a => !blackList.Contains(a.WithoutPeerId()))
                .Select(a => a.WithPeerId(remote.Id))
                .Distinct()
                .ToArray();
            if (possibleAddresses.Length == 0)
            {
                throw new Exception($"{remote} has no known or reachable address.");
            }

            // Try the various addresses in parallel.  The first one to complete wins.
            PeerConnection connection = null;
            try
            {
                using (var timeout = new CancellationTokenSource(TransportConnectionTimeout))
                using (var cts = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token, cancel))
                {
                    var attempts = possibleAddresses
                        .Select(a => DialAsync(remote, a, cts.Token));
                    connection = await TaskHelper.WhenAnyResultAsync(attempts, cts.Token).ConfigureAwait(false);
                    cts.Cancel(); // stop other dialing tasks.
                }
            }
            catch (Exception e)
            {
                var attemped = string.Join(", ", possibleAddresses.Select(a => a.ToString()));
                log.Trace($"Cannot dial {attemped}");
                throw new Exception($"Cannot dial {remote}.", e);
            }

            // Do the connection handshake.
            try
            {
                OtherPeers.MountProtocols(connection);
                IEnumerable<IEncryptionProtocol> security = OtherPeers.EncryptionProtocols;
                await connection.InitiateAsync(security, cancel).ConfigureAwait(false);
                await connection.MuxerEstablished.Task.ConfigureAwait(false);

                Identify1 identify = OtherPeers.IdentifyProtocol;
                await identify.GetRemotePeerAsync(connection, cancel).ConfigureAwait(false);
                //FIXME WHY is the above discarded
            }
            catch (Exception)
            {
                connection.Dispose();
                throw;
            }

            var actual = Manager.Add(connection);
            if (actual == connection)
            {
                ConnectionEstablished?.Invoke(this, connection);
            }

            return actual;

        }

        async Task<PeerConnection> DialAsync(Peer remote, MultiAddress addr, CancellationToken cancel)
        {
            // TODO: HACK: Currenty only the ipfs/p2p is supported.
            // short circuit to make life faster.
            if (addr.Protocols.Count != 3
                || !(addr.Protocols[2].Name == "ipfs" || addr.Protocols[2].Name == "p2p"))
            {
                throw new Exception($"Cannnot dial; unknown protocol in '{addr}'.");
            }

            // Establish the transport stream.
            Stream stream = null;
            foreach (var protocol in addr.Protocols)
            {
                cancel.ThrowIfCancellationRequested();
                if (TransportRegistry.Transports.TryGetValue(protocol.Name, out Func<IPeerTransport> transport))
                {
                    stream = await transport().ConnectAsync(addr, cancel).ConfigureAwait(false);
                    if (cancel.IsCancellationRequested)
                    {
                        stream?.Dispose();
                        continue;
                    }
                    break;
                }
            }
            if (stream == null)
            {
                throw new Exception("Missing a known transport protocol name.");
            }

            // Build the connection.
            var connection = new PeerConnection
            {
                IsIncoming = false,
                LocalPeer = LocalPeer,
                // TODO: LocalAddress
                LocalPeerKey = LocalPeerKey,
                RemotePeer = remote,
                RemoteAddress = addr,
                Stream = stream
            };

            // Are we communicating to a private network?
            if (NetworkProtector != null)
            {
                connection.Stream = await NetworkProtector.ProtectAsync(connection).ConfigureAwait(false);
            }


            return connection;
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
        public async Task<PeerConnection> ConnectAsync(Peer peer, CancellationToken cancel = default(CancellationToken))
        {
            if (!IsRunning)
            {
                throw new Exception("The swarm is not running.");
            }

            // If connected and still open, then use the existing connection.
            if (Manager.TryGet(peer, out PeerConnection conn))
            {
                return conn;
            }

            // Use a current connection attempt to the peer or create a new one.
            try
            {
                using (var cts = CancellationTokenSource.CreateLinkedTokenSource(swarmCancellation.Token, cancel))
                {
                    return await pendingConnections
                        .GetOrAdd(peer, (key) => new AsyncLazy<PeerConnection>(() => DialAsync(peer, peer.Addresses, cts.Token)))
                        .ConfigureAwait(false);
                }
            }
            catch (Exception)
            {
                PeerNotReachable?.Invoke(this, peer);
                throw;
            }
            finally
            {
                pendingConnections.TryRemove(peer, out AsyncLazy<PeerConnection> _);
            }
        }

    }
}
