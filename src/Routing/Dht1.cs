﻿using Common.Logging;
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
        ///   Provides access to other peers.
        /// </summary>
        public Swarm Swarm { get; set; }

        /// <summary>
        ///  Routing information on peers.
        /// </summary>
        public RoutingTable RoutingTable;

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

        /// <inheritdoc />
        public async Task ProcessMessageAsync(PeerConnection connection, Stream stream, CancellationToken cancel = default(CancellationToken))
        {
            while (true)
            {
                var request = await ProtoBufHelper.ReadMessageAsync<DhtMessage>(stream, cancel);

                log.Debug($"got {request.Type} from {connection.RemotePeer}");
                var response = new DhtMessage
                {
                    Type = request.Type,
                    ClusterLevelRaw = request.ClusterLevelRaw
                };
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
                    default:
                        log.Debug($"unknown {request.Type} from {connection.RemotePeer}");
                        // TODO: Should we close the stream?
                        continue;
                }
                if (response != null)
                {
                    ProtoBuf.Serializer.SerializeWithLengthPrefix(stream, response, PrefixStyle.Base128);
                    await stream.FlushAsync(cancel);
                }
            }
        }

        /// <inheritdoc />
        public Task StartAsync()
        {
            log.Debug("Starting");

            RoutingTable = new RoutingTable(Swarm.LocalPeer);
            Swarm.AddProtocol(this);
            Swarm.PeerDiscovered += Swarm_PeerDiscovered;
            foreach (var peer in Swarm.KnownPeers)
            {
                RoutingTable.Add(peer);
            }

            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public Task StopAsync()
        {
            log.Debug("Stopping");

            Swarm.RemoveProtocol(this);
            Swarm.PeerDiscovered -= Swarm_PeerDiscovered;

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

        /// <inheritdoc />
        public async Task<Peer> FindPeerAsync(MultiHash id, CancellationToken cancel = default(CancellationToken))
        {
            // Can always find self.
            if (Swarm.LocalPeer.Id == id)
                return Swarm.LocalPeer;

            // Maybe the swarm knows about it.
            var found = Swarm.KnownPeers.FirstOrDefault(p => p.Id == id);
            if (found != null && found.Addresses.Count() > 0)
                return found;

            // Ask our peers for information on the requested peer.
            var dquery = new DistributedQuery<Peer>
            {
                QueryType = MessageType.FindNode,
                QueryKey = id,
                Dht = this,
                AnswersNeeded = 1
            };
            await dquery.RunAsync(cancel);
            if (dquery.Answers.Count == 0)
            {
                throw new KeyNotFoundException($"Cannot locate peer '{id}'.");
            }
            return dquery.Answers.First();
        }

        /// <inheritdoc />
        public Task ProvideAsync(Cid cid, bool advertise = true, CancellationToken cancel = default(CancellationToken))
        {
            throw new NotImplementedException("DHT ProvideAsync");
        }

        /// <inheritdoc />
        public async Task<IEnumerable<Peer>> FindProvidersAsync(
            Cid id,
            int limit = 20,
            Action<Peer> action = null,
            CancellationToken cancel = default(CancellationToken))
        {
            var dquery = new DistributedQuery<Peer>
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
            await dquery.RunAsync(cancel);
            return dquery.Answers.Take(limit);
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
            var peerId = new MultiHash(request.Key);

            // Do we know the peer?.
            Peer found = null;
            if (Swarm.LocalPeer.Id == peerId)
            {
                found = Swarm.LocalPeer;
            }
            else
            {
                found = Swarm.KnownPeers.FirstOrDefault(p => p.Id == peerId);
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
        ///   Process a find node request.
        /// </summary>
        public DhtMessage ProcessGetProviders(DhtMessage request, DhtMessage response)
        {
            // TODO: Find a provider for the content.

            // Also return the closest peers
            return ProcessFindNode(request, response);
        }
    }
}