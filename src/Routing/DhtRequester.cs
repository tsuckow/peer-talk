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
                var result = await MessagePeerAsync(peer, new DhtMessage { Type = MessageType.FindNode, Key = id.Digest }, token);
                return result.CloserPeers.ToDictionary(p => p.MultiHash, p => p.MultiAddresses);
            } catch(Exception)
            {
                return ImmutableDictionary.Create<MultiHash, IEnumerable<MultiAddress>>();
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
                using (var cts = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token,token))
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

        void ProcessProviders(DhtPeerMessage[] providers)
        {
            if (providers == null)
                return;

            foreach (var provider in providers)
            {
                try
                {
                    log.Info($"Provider: {provider}");
                }
                catch (Exception) //Fixme RegisterPeer should throw a custom exception when not allowed
                {
                    continue;
                }
            }
        }

        void ProcessCloserPeers(DhtPeerMessage[] closerPeers)
        {
            if (closerPeers == null)
                return;
            foreach (var closer in closerPeers)
            {
                    log.Info($"Closer: {closer}");
            }
        }
    }
}
