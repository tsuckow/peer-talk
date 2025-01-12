﻿using Common.Logging;
using Ipfs;
using Noise;
using PeerTalk.Cryptography;
using PeerTalk.Protocols;
using Semver;
using System;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;

namespace PeerTalk.SecureCommunication.Noise
{
    /// <summary>
    ///   Creates a secure connection with a peer.
    /// </summary>
    public class Noise : IEncryptionProtocol
    {
        static ILog log = LogManager.GetLogger(typeof(Noise));

        /// <inheritdoc />
        public string Name { get; } = "noise";

        /// <inheritdoc />
        public SemVersion Version { get; } = new SemVersion(1, 0);

        private PeerList peerList;

        /// <inheritdoc />
        public override string ToString()
        {
            return $"/{Name}";
        }

        /// <summary>
        /// Creates a noise protocol encryptor
        /// </summary>
        /// <param name="peerList">PeerList for instantiating peers</param>
        public Noise(PeerList peerList)
        {
            this.peerList = peerList;
        }

        private static byte[] payloadSigPrefix = System.Text.Encoding.UTF8.GetBytes("noise-libp2p-static-key:");

        /// <inheritdoc />
        public async Task ProcessMessageAsync(PeerConnection connection, Stream stream, CancellationToken cancel = default(CancellationToken))
        {
            await EncryptAsync(connection, cancel).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task<Stream> EncryptAsync(PeerConnection connection, CancellationToken cancel = default(CancellationToken))
        {
            var stream = connection.Stream;
            connection.RemotePeer = connection.RemotePeer ?? new Peer();

            log.Info($"Setting up noise protocol for {connection.RemotePeer.Id} Incoming: {connection.IsIncoming}");


            var protocol = new Protocol(
              HandshakePattern.XX,
              CipherFunction.ChaChaPoly,
              HashFunction.Sha256,
              PatternModifiers.None
            );

            /*
             * In a nutshell
             * 
             * We generate a new public keypair for this connection (Our "static" key) and establish a connection to another peer.
             * 
             * If we started the connection, they send us their identity public key and a signature signed with the identity key of
             * their salted static public key so we can verify this connection could only be estabilshed by that identity.
             * 
             * We then do the same, sending our identity public key and sign with our identity key our salted static public key
             * 
             * Note: The salt, though constant, makes collision attacks more difficult.
             */

            try
            {
                using (var kp = KeyPair.Generate())
                {
                    var streambuffer = new byte[Protocol.MaxMessageLength];
                    var plaintextbuffer = new byte[Protocol.MaxMessageLength];

                    using (var state = protocol.Create(initiator: !connection.IsIncoming, s: kp.PrivateKey))
                    {
                        if (!connection.IsIncoming)
                        {
                            {
                                var (bytesWritten, _, _) = state.WriteMessage(null, streambuffer);
                                await WriteStreamMessageAsync(stream, streambuffer, bytesWritten);
                            }

                            // Receive the second handshake message from the server.
                            var received = await ReadStreamMessageAsync(stream, streambuffer, Protocol.MaxMessageLength);
                            var (bytesRead, _, _) = state.ReadMessage(new ReadOnlySpan<byte>(streambuffer, 0, received), plaintextbuffer);

                            using (var incomingstream = new MemoryStream(plaintextbuffer, 0, bytesRead, writable: false)) {
                                var peerPayload = ProtoBuf.Serializer.Deserialize<NoiseHandshakePayload>(incomingstream);
                                ValidatePayload(connection, state, peerPayload);
                            }

                            log.Info($"Validated the peer identity with Noise Protocol: {connection.RemotePeer.Id}");

                            // Send third step in handshake
                            var myPayload = GeneratePayload(connection, state, kp.PublicKey);

                            using (var outgoingstream = new MemoryStream(plaintextbuffer, 0, Protocol.MaxMessageLength, writable: true))
                            {
                                ProtoBuf.Serializer.Serialize(outgoingstream, myPayload);
                                var (bytesWritten, _, transport) = state.WriteMessage(new ReadOnlySpan<byte>(plaintextbuffer, 0, Convert.ToInt32(outgoingstream.Position)), streambuffer);
                                await WriteStreamMessageAsync(stream, streambuffer, bytesWritten);

                                log.Info($"Noise Handshake Done {connection.RemotePeer.Id}");

                                var secureStream = new NoiseStream(transport, stream);
                                connection.Stream = secureStream;
                                connection.SecurityEstablished.SetResult(true);
                                return secureStream;
                            }
                        }
                        else
                        {
                            try {
                                {
                                    var received = await ReadStreamMessageAsync(stream, streambuffer, Protocol.MaxMessageLength);
                                    var (_, _, _) = state.ReadMessage(new ReadOnlySpan<byte>(streambuffer, 0, received), plaintextbuffer);
                                }

                                // Send second step in handshake
                                var myPayload = GeneratePayload(connection, state, kp.PublicKey);

                                using (var outgoingstream = new MemoryStream(plaintextbuffer, 0, Protocol.MaxMessageLength, writable: true))
                                {
                                    ProtoBuf.Serializer.Serialize(outgoingstream, myPayload);
                                    log.Error($"Lengths: {outgoingstream.Length} {outgoingstream.Position}");
                                    var (bytesWritten, _, _) = state.WriteMessage(new ReadOnlySpan<byte>(plaintextbuffer, 0, Convert.ToInt32(outgoingstream.Position)), streambuffer);
                                    await WriteStreamMessageAsync(stream, streambuffer, bytesWritten);
                                }

                                // Receive the third handshake message from the server.
                                {
                                    var received = await ReadStreamMessageAsync(stream, streambuffer, Protocol.MaxMessageLength);
                                    var (bytesRead, _, transport) = state.ReadMessage(new ReadOnlySpan<byte>(streambuffer, 0, received), plaintextbuffer);

                                    using (var incomingstream = new MemoryStream(plaintextbuffer, 0, bytesRead, writable: false))
                                    {
                                        var peerPayload = ProtoBuf.Serializer.Deserialize<NoiseHandshakePayload>(incomingstream);
                                        ValidatePayload(connection, state, peerPayload);
                                    }

                                    log.Info($"Validated incoming peer identity with Noise Protocol: {connection.RemotePeer.Id}");

                                    var secureStream = new NoiseStream(transport, stream);
                                    connection.Stream = secureStream;
                                    connection.SecurityEstablished.SetResult(true);
                                    return secureStream;
                                }
                            }
                            catch (Exception e)
                            {
                                log.Error($"Something incoming failed {e.Message}", e);
                                throw;
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                log.Error($"Something failed {e.Message}", e);
                throw;
            }
        }

        /**
         * Writes a message to the stream and prefixes it with the message length
         */
        private static async Task WriteStreamMessageAsync(Stream stream, byte[] buffer, int bytes)
        {
            if (bytes > UInt16.MaxValue)
            {
                throw new ArgumentOutOfRangeException("Message size exceeds uint16 bytes");
            }

            var prefix = BitConverter.GetBytes((UInt16)bytes);
            //We Want Big Endian
            if (BitConverter.IsLittleEndian)
                Array.Reverse(prefix);

            await stream.WriteAsync(prefix, 0, 2);

            await stream.WriteAsync(buffer, 0, bytes);
            await stream.FlushAsync().ConfigureAwait(false);
        }

        /**
         * Reads a message from the stream which is prefixed with the message length
         */
        private static async Task<int> ReadStreamMessageAsync(Stream stream, byte[] buffer, UInt16 bufferSize)
        {
            var prefix = new byte[2];

            if (await stream.ReadAsync(prefix, 0, 1) == 0)
            {
                throw new Exception("Connection Closed");
            }
            if (await stream.ReadAsync(prefix, 1, 1) == 0)
            {
                throw new Exception("Connection Closed");
            }

            //We Want Big Endian
            if (BitConverter.IsLittleEndian)
                Array.Reverse(prefix);
            var messageSize = BitConverter.ToUInt16(prefix, 0);

            if (bufferSize < messageSize)
            {
                throw new Exception($"Recieved message ({messageSize}) larger than buffer ({bufferSize})");
            }

            var readTotal = 0;
            while( messageSize > readTotal )
            {
                var read = await stream.ReadAsync(buffer, readTotal, messageSize - readTotal);
                if (read == 0)
                {
                    throw new Exception("Connection Closed Mid-read");
                }
                readTotal += read;
            }

            return readTotal;
        }

        private static MultiHash PeerKeyToId(byte[] key)
        {
            var ridAlg = (key.Length <= 48) ? "identity" : "sha2-256";
            return MultiHash.ComputeHash(key, ridAlg);
        }

        private static void ValidatePayload(PeerConnection connection, HandshakeState state, NoiseHandshakePayload payload)
        {
            var remotePeer = connection.RemotePeer;

            var remoteId = PeerKeyToId(payload.IdentityKey);
            if (remotePeer.Id == null)
            {
                remotePeer.Id = remoteId;
            }
            else if (remoteId != remotePeer.Id)
            {
                throw new Exception($"Expected peer '{remotePeer.Id}', got '{remoteId}'");
            }

            var peerStaticKey = state.RemoteStaticPublicKey;
            try
            {
                var peerIdentityKey = Key.CreatePublicKeyFromIpfs(payload.IdentityKey);

                using (var ms = new MemoryStream())
                {
                    ms.Write(payloadSigPrefix, 0, payloadSigPrefix.Length);
                    ms.Write(peerStaticKey.ToArray(), 0, peerStaticKey.Length);
                    peerIdentityKey.Verify(ms.ToArray(), payload.IdentitySig);
                }

                remotePeer.PublicKey = Convert.ToBase64String(payload.IdentityKey);
            } catch (Exception e)
            {
                log.Error($"Identity Verification Failure: {e.Message}", e);
                throw;
            }
        }

        private static NoiseHandshakePayload GeneratePayload(PeerConnection connection, HandshakeState state, byte[] myStaticPublicKey)
        {
            var payload = new NoiseHandshakePayload();

            payload.Data = null;
            payload.IdentityKey = Convert.FromBase64String(connection.LocalPeer.PublicKey);

            using (var ms = new MemoryStream())
            {
                ms.Write(payloadSigPrefix, 0, payloadSigPrefix.Length);
                ms.Write(myStaticPublicKey, 0, myStaticPublicKey.Length);
                payload.IdentitySig = connection.LocalPeerKey.Sign(ms.ToArray());
            }

            return payload;
        }
    }
}
