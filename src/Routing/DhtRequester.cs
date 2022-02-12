using Common.Logging;
using Ipfs;
using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PeerTalk.Routing
{
    internal class DhtCloserProviders
    {
        public IDictionary<MultiHash, IEnumerable<MultiAddress>> Closer = ImmutableDictionary.Create<MultiHash, IEnumerable<MultiAddress>>();
        public IDictionary<MultiHash, IEnumerable<MultiAddress>> Providers = ImmutableDictionary.Create<MultiHash, IEnumerable<MultiAddress>>();
    }

    internal class DhtRequester
    {
        static ILog log = LogManager.GetLogger("PeerTalk.Routing.DhtRequester");

        const string PROTOCOL = "/ipfs/kad/1.0.0";

        /// <summary>
        ///   The maximum number of peers that can be queried at one time
        ///   for all distributed queries.
        /// </summary>
        static SemaphoreSlim askCount = new SemaphoreSlim(128);

        /// <summary>
        ///   The maximum time spent on waiting for an answer from a peer.
        /// </summary>
        static readonly TimeSpan askTime = TimeSpan.FromSeconds(10);

        Switchboard Switchboard;

        internal DhtRequester(Switchboard switchboard)
        {
            Switchboard = switchboard;
        }

        public async Task<IDictionary<MultiHash, IEnumerable<MultiAddress>>> ClosestAddressAsync(Peer peer, MultiHash id, CancellationToken token = default)
        {
            try
            {
                var result = await MessagePeerAsync(peer, new DhtMessage { Type = MessageType.FindNode, Key = id.ToArray(), }, token);
                return result.CloserPeers.ToDictionary(p => p.MultiHash, p => p.MultiAddresses);
            }
            catch (Exception)
            {
                return ImmutableDictionary.Create<MultiHash, IEnumerable<MultiAddress>>();
            }
        }

        public async Task<DhtCloserProviders> RequestProvidersAsync(Peer peer, Cid id, CancellationToken token = default)
        {
            try
            {
                var result = await MessagePeerAsync(peer, new DhtMessage { Type = MessageType.GetProviders, Key = id.Hash.ToArray() }, token);
                var ret = new DhtCloserProviders();
                if (result.CloserPeers != null)
                {
                    ret.Closer = result.CloserPeers.ToDictionary(p => p.MultiHash, p => p.MultiAddresses);
                }
                if (result.ProviderPeers != null)
                {
                    ret.Providers = result.ProviderPeers.ToDictionary(p => p.MultiHash, p => p.MultiAddresses);
                }

                return ret;
            }
            catch (Exception)
            {
                return new DhtCloserProviders();
            }
        }

        public async Task<DhtMessage> MessagePeerAsync(Peer peer, DhtMessage queryMessage, CancellationToken token = default)
        {
            await askCount.WaitAsync(token).ConfigureAwait(false);
            var start = DateTime.Now;
            log.Debug($"DHT Ask {peer} {queryMessage.Type}");
            try
            {
                using (var timeout = new CancellationTokenSource(askTime))
                using (var cts = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token, token))
                using (var stream = await Switchboard.DialAsync(peer, PROTOCOL, cts.Token).ConfigureAwait(false))
                {
                    // Send the KAD query and get a response.
                    ProtoBuf.Serializer.SerializeWithLengthPrefix(stream, queryMessage, PrefixStyle.Base128);
                    await stream.FlushAsync(cts.Token).ConfigureAwait(false);
                    var response = await ProtoBufHelper.ReadMessageAsync<DhtMessage>(stream, cts.Token).ConfigureAwait(false);

                    var time = DateTime.Now - start;
                    log.Debug($"DHT OK {peer} ({time.TotalMilliseconds} ms)");

                    return response;
                }
            }
            catch (Exception e)
            {
                var time = DateTime.Now - start;
                log.Warn($"DHT Failed {peer} ({time.TotalMilliseconds} ms) - {e.Message}");

                throw;
            }
            finally
            {
                askCount.Release();
            }
        }
    }
}
