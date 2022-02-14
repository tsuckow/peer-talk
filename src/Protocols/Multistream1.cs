using Common.Logging;
using PeerTalk.Multiplex;
using Semver;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PeerTalk.Protocols
{
    /// <summary>
    ///   A protocol to select other protocols.
    /// </summary>
    /// <seealso href="https://github.com/multiformats/multistream-select"/>
    public class Multistream1 : IPeerProtocol
    {
        static ILog log = LogManager.GetLogger(typeof(Multistream1));

        /// <inheritdoc />
        public string Name { get; } = "multistream";

        /// <inheritdoc />
        public SemVersion Version { get; } = new SemVersion(1, 0);

        /// <inheritdoc />
        public override string ToString()
        {
            return $"/{Name}/{Version}";
        }

        /// <summary>
        ///  
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="stream"></param>
        /// <param name="protocol"></param>
        /// <param name="cancel"></param>
        /// <returns></returns>
        public async Task<ProtocolType> NegotiateProtocolAsync<ProtocolType>(PeerConnection connection, Stream stream, ProtocolType protocol, CancellationToken cancel = default) where ProtocolType : IPeerProtocol
        {
            return await NegotiateProtocolAsync(connection, stream, new[] { protocol }, cancel);
        }

        /// <summary>
        /// Negotiates 
        /// </summary>
        /// <typeparam name="ProtocolType"></typeparam>
        /// <param name="connection"></param>
        /// <param name="stream"></param>
        /// <param name="protocols"></param>
        /// <param name="cancel"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public async Task<ProtocolType> NegotiateProtocolAsync<ProtocolType>(PeerConnection connection, Stream stream, IEnumerable<ProtocolType> protocols, CancellationToken cancel = default) where ProtocolType : IPeerProtocol
        {
            if(!await EstablishProtocolAsync(this, stream, cancel).ConfigureAwait(false))
            {
                throw new Exception($"Unexpected response doing multistream-select handshake.");
            }

            // Find the first security protocol that is also supported by the remote.
            var exceptions = new List<Exception>();
            foreach (var protocol in protocols)
            {
                if (await EstablishProtocolAsync(protocol, stream, cancel).ConfigureAwait(false)) {
                    return protocol;
                }
            }

            throw new Exception($"Failed to negotiate with {connection.RemoteAddress}, protocols not supported: " + String.Join(", ", protocols.Select(p => p.ToString())));
        }


        /// <summary>
        ///   Sends a protocol header and checks if the response matches
        /// </summary>
        /// <param name="protocol"></param>
        /// <param name="stream"></param>
        /// <param name="cancel"></param>
        /// <returns></returns>
        private async Task<bool> EstablishProtocolAsync(IPeerProtocol protocol, Stream stream, CancellationToken cancel = default(CancellationToken))
        {
            var name = protocol.ToString();
            await Message.WriteAsync(name, stream, cancel).ConfigureAwait(false);
            var result = await Message.ReadStringAsync(stream, cancel).ConfigureAwait(false);

            if (result == name)
            {
                return true;
            } else if (result == "na")
            {
                return false;
            } else
            {
                throw new Exception($"Unexpected response negotiating protocol '{name}' via multistream-select.");
            }
        }

        /// <inheritdoc />
        public async Task ProcessMessageAsync(PeerConnection connection, Stream stream, CancellationToken cancel = default(CancellationToken))
        {
            var msg = await Message.ReadStringAsync(stream, cancel).ConfigureAwait(false);

            if(stream is Substream substream)
            {
                substream.Name = msg;
            }

            // TODO: msg == "ls"
            if (msg == "ls")
            {
                throw new NotImplementedException("multistream ls");
            }

            // Switch to the specified protocol
            if (!connection.Protocols.TryGetValue(msg, out Func<PeerConnection, Stream, CancellationToken, Task> protocol))
            {
                await Message.WriteAsync("na", stream, cancel).ConfigureAwait(false);
                return;
            }

            // Ack protocol switch
            log.Debug("switching to " + msg);
            await Message.WriteAsync(msg, stream, cancel).ConfigureAwait(false);

            // Process protocol message.
            await protocol(connection, stream, cancel).ConfigureAwait(false);
        }

    }
}
