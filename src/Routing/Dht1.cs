using Common.Logging;
using Ipfs;
using Ipfs.CoreApi;
using PeerTalk.Protocols;
using ProtoBuf;
using Semver;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Alethic.Kademlia;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Logging.Abstractions;
using Alethic.Kademlia.InMemory;

namespace PeerTalk.Routing
{
    /// <summary>
    ///   DHT Protocol version 1.0
    /// </summary>
    public class Dht1 : IPeerProtocol, IService, IPeerRouting, IContentRouting
    {
        static ILog log = LogManager.GetLogger(typeof(Dht1));

        /// <inheritdoc />
        public string Name { get; } = "ipfs/kad";

        /// <inheritdoc />
        public SemVersion Version { get; } = new SemVersion(1, 0);

        /// <summary>
        ///  Routing information on peers.
        /// </summary>
        public RoutingTable RoutingTable;

        /// <summary>
        ///   Peers that can provide some content.
        /// </summary>
        public ContentRouter ContentRouter;

        /// <summary>
        ///   The number of closer peers to return.
        /// </summary>
        /// <value>
        ///   Defaults to 20.
        /// </value>
        public int CloserPeerCount { get; set; } = 20;

        /// <summary>
        ///   Raised when the DHT is stopped.
        /// </summary>
        /// <seealso cref="StopAsync"/>
        public event EventHandler Stopped;

        /// <inheritdoc />
        public override string ToString()
        {
            return $"/{Name}/{Version}";
        }

        internal readonly Peer LocalPeer;
        internal PeerList OtherPeers;
        internal Switchboard Switchboard;

        private KFixedTableRouter<KNodeId256> Router;
        private KRequestHandler<KNodeId256> Handler;

        /// <summary>
        ///  Instantiates a DHT
        /// </summary>
        /// <param name="localPeer"></param>
        /// <param name="otherPeers"></param>
        /// <param name="switchboard"></param>
        public Dht1(Peer localPeer, PeerList otherPeers, Switchboard switchboard)
        {
            LocalPeer = localPeer;
            OtherPeers = otherPeers;
            Switchboard = switchboard;
            var hostOptions = new KHostOptions<KNodeId256> {
                NodeId = new KNodeId256(localPeer.Id.Digest),
                NetworkId = 0,
                Endpoints = new Uri[] { }
            };
            var logger = NullLogger.Instance;
            var host = new KHost<KNodeId256>(Options.Create(hostOptions), logger);
            var invokerPolicy = new KInvokerPolicy<KNodeId256>(logger);
            var invoker = new KInvoker<KNodeId256>(host, invokerPolicy);
            Router = new KFixedTableRouter<KNodeId256>(Options.Create(new KFixedTableRouterOptions { }), host, invoker, logger);
            var lookup = new KLookup<KNodeId256>(host, Router, invoker, logger);
            var store = new KInMemoryStore<KNodeId256>(host, Router, invoker, lookup, logger);
            Handler = new KRequestHandler<KNodeId256>(host, Router, store, logger);
            //KConnector?
            //KRefresher?
            //KStaticDiscovery?

            //foreach (var peer in Swarm.KnownPeers)
            //{
            //    RoutingTable.Add(peer);
            //}
        }

        /// <inheritdoc />
        public async Task ProcessMessageAsync(PeerConnection connection, Stream stream, CancellationToken cancel = default(CancellationToken))
        {
            while (true)
            {
                var request = await ProtoBufHelper.ReadMessageAsync<DhtMessage>(stream, cancel).ConfigureAwait(false);

                log.Debug($"got {request.Type} from {connection.RemotePeer}");
                var response = new DhtMessage
                {
                    Type = request.Type,
                    ClusterLevelRaw = request.ClusterLevelRaw
                };

                //https://github.com/alethic/Alethic.Kademlia/blob/0bd2ad122bc7fd6787d4a9ce5077b57e0c0e24f7/Alethic.Kademlia/Network/Udp/KUdpServer.cs
                switch (request.Type)
                {
                    case MessageType.Ping:
                        response = ProcessPing(request, response);
                        break;
                    case MessageType.FindNode:
                        response = ProcessFindNode(request, response);
                        break;
                    case MessageType.GetProviders:
                        response = ProcessGetProviders(request, response);
                        break;
                    case MessageType.AddProvider:
                        response = ProcessAddProvider(connection.RemotePeer, request, response);
                        break;
                    default:
                        log.Debug($"unknown {request.Type} from {connection.RemotePeer}");
                        // TODO: Should we close the stream?
                        continue;
                }
                if (response != null)
                {
                    ProtoBuf.Serializer.SerializeWithLengthPrefix(stream, response, PrefixStyle.Base128);
                    await stream.FlushAsync(cancel).ConfigureAwait(false);
                }
            }
        }

        /// <inheritdoc />
        public Task StartAsync()
        {
            log.Debug("Starting");

           //KRefresher?

            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public Task StopAsync()
        {
            log.Debug("Stopping");

            Stopped?.Invoke(this, EventArgs.Empty);

            return Task.CompletedTask;
        }

        /// <summary>
        ///   The swarm has discovered a new peer, update the routing table.
        /// </summary>
        void Swarm_PeerDiscovered(object sender, Peer e)
        {
            RoutingTable.Add(e);
        }

        /// <summary>
        ///   The swarm has removed a peer, update the routing table.
        /// </summary>
        private void Swarm_PeerRemoved(object sender, Peer e)
        {
            RoutingTable.Remove(e);
        }

        /// <inheritdoc />
        public async Task<Peer> FindPeerAsync(MultiHash id, CancellationToken cancel = default(CancellationToken))
        {
            // Can always find self.
            if (LocalPeer.Id == id)
                return LocalPeer;

            // Maybe the swarm knows about it.
            var found = OtherPeers.Peers.FirstOrDefault(p => p.Id == id);
            if (found != null && found.Addresses.Count() > 0)
                return found;

            // Ask our peers for information on the requested peer.
            var dquery = new DistributedQuery
            {
                QueryType = MessageType.FindNode,
                QueryKey = id,
                Dht = this,
                AnswersNeeded = 1
            };
            await dquery.RunAsync(cancel).ConfigureAwait(false);

            // If not found, return the closest peer.
            if (dquery.Answers.Count() == 0)
            {
                return RoutingTable.NearestPeers(id).FirstOrDefault();
            }

            return dquery.Answers.First();
        }

        /// <inheritdoc />
        public Task ProvideAsync(Cid cid, bool advertise = true, CancellationToken cancel = default(CancellationToken))
        {
            ContentRouter.Add(cid, LocalPeer.Id);
            if (advertise)
            {
                Advertise(cid);
            }

            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public async Task<IEnumerable<Peer>> FindProvidersAsync(
            Cid id,
            int limit = 20,
            Action<Peer> action = null,
            CancellationToken cancel = default(CancellationToken))
        {
            var dquery = new DistributedQuery
            {
                QueryType = MessageType.GetProviders,
                QueryKey = id.Hash,
                Dht = this,
                AnswersNeeded = limit,
            };
            if (action != null)
            {
                dquery.AnswerObtained += (s, e) => action.Invoke(e);
            }

            // Add any providers that we already know about.
            var providers = ContentRouter
                .Get(id)
                .Select(OtherPeers.ResolvePeer);
            foreach (var provider in providers)
            {
                dquery.AddAnswer(provider);
            }

            // Ask our peers for more providers.
            if (limit > dquery.Answers.Count())
            {
                await dquery.RunAsync(cancel).ConfigureAwait(false);
            }

            return dquery.Answers.Take(limit);
        }

        /// <summary>
        ///   Advertise that we can provide the CID to the X closest peers
        ///   of the CID.
        /// </summary>
        /// <param name="cid">
        ///   The CID to advertise.
        /// </param>
        /// <remarks>
        ///   This starts a background process to send the AddProvider message
        ///   to the 4 closest peers to the <paramref name="cid"/>.
        /// </remarks>
        public void Advertise(Cid cid)
        {
            _ = Task.Run(async () =>
            {
                int advertsNeeded = 4;
                var message = new DhtMessage
                {
                    Type = MessageType.AddProvider,
                    Key = cid.Hash.ToArray(),
                    ProviderPeers = new DhtPeerMessage[]
                    {
                        new DhtPeerMessage
                        {
                            Id = LocalPeer.Id.ToArray(),
                            Addresses = LocalPeer.Addresses
                                .Select(a => a.WithoutPeerId().ToArray())
                                .ToArray()
                        }
                    }
                };
                var peers = RoutingTable
                    .NearestPeers(cid.Hash)
                    .Where(p => p != LocalPeer);   
                foreach (var peer in peers)
                {
                    try
                    {
                        //FIXME
                        await Task.Delay(1);
                        //using (var stream = await Swarm.DialAsync(peer, this.ToString()))
                        //{
                        //    ProtoBuf.Serializer.SerializeWithLengthPrefix(stream, message, PrefixStyle.Base128);
                        //    await stream.FlushAsync();
                        //}
                        if (--advertsNeeded == 0)
                            break;
                    }
                    catch (Exception)
                    {
                        // eat it.  This is fire and forget.
                    }
                }
            });
        }

        /// <summary>
        ///   Process a ping request.
        /// </summary>
        /// <remarks>
        ///   Simply return the <paramref name="request"/>.
        /// </remarks>
        DhtMessage ProcessPing(DhtMessage request, DhtMessage response)
        {
            return request;
        }

        /// <summary>
        ///   Process a find node request.
        /// </summary>
        public DhtMessage ProcessFindNode(DhtMessage request, DhtMessage response)
        {
            // Some random walkers generate a random Key that is not hashed.
            MultiHash peerId;
            try
            {
                peerId = new MultiHash(request.Key);
            }
            catch (Exception)
            {
                log.Error($"Bad FindNode request key {request.Key.ToHexString()}");
                peerId = MultiHash.ComputeHash(request.Key);
            }

            // Do we know the peer?.
            Peer found = null;
            if (LocalPeer.Id == peerId)
            {
                found = LocalPeer;
            }
            else
            {
                found = OtherPeers.Peers.FirstOrDefault(p => p.Id == peerId);
            }

            // Find the closer peers.
            var closerPeers = new List<Peer>();
            if (found != null)
            {
                closerPeers.Add(found);
            }
            else
            {
                closerPeers.AddRange(RoutingTable.NearestPeers(peerId).Take(CloserPeerCount));
            }

            // Build the response.
            response.CloserPeers = closerPeers
                .Select(peer => new DhtPeerMessage
                {
                    Id = peer.Id.ToArray(),
                    Addresses = peer.Addresses.Select(a => a.WithoutPeerId().ToArray()).ToArray()
                })
                .ToArray();

            if (log.IsDebugEnabled)
                log.Debug($"returning {response.CloserPeers.Length} closer peers");
            return response;
        }

        /// <summary>
        ///   Process a get provider request.
        /// </summary>
        public DhtMessage ProcessGetProviders(DhtMessage request, DhtMessage response)
        {
            // Find providers for the content.
            var cid = new Cid { Hash = new MultiHash(request.Key) };
            response.ProviderPeers = ContentRouter
                .Get(cid)
                .Select(pid =>
                {
                    var peer = OtherPeers.ResolvePeer(pid);
                    return new DhtPeerMessage
                    {
                        Id = peer.Id.ToArray(),
                        Addresses = peer.Addresses.Select(a => a.WithoutPeerId().ToArray()).ToArray()
                    };
                })
                .Take(20)
                .ToArray();

            // Also return the closest peers
            return ProcessFindNode(request, response);
        }

        /// <summary>
        ///   Process an add provider request.
        /// </summary>
        public DhtMessage ProcessAddProvider(Peer remotePeer, DhtMessage request, DhtMessage response)
        {
            if (request.ProviderPeers == null)
            {
                return null;
            }
            Cid cid;
            try
            {
                cid = new Cid { Hash = new MultiHash(request.Key) };
            }
            catch (Exception)
            {
                log.Error($"Bad AddProvider request key {request.Key.ToHexString()}");
                return null;
            }
            var providers = request.ProviderPeers
                .Select(p => {
                    OtherPeers.RegisterPeer(p.MultiHash, out Peer peer, p.MultiAddresses);
                    return peer;
                   })
                .Where(p => p != null)
                .Where(p => p == remotePeer)
                .Where(p => p.Addresses.Count() > 0);
            foreach (var provider in providers)
            {
                ContentRouter.Add(cid, provider.Id);
            };

            // There is no response for this request.
            return null;
        }

    }
}