using PeerTalk.Protocols;
using System.IO;

namespace PeerTalk
{
    /// <summary>
    /// Factory for wrapping a stream to include the negotiated protocol
    /// </summary>
    public static class ProtocolStream
    {
        /// <summary>
        ///  Wraps a stream to include the negotiated protocol
        /// </summary>
        /// <typeparam name="Protocol">Type of negotiated protocol</typeparam>
        /// <param name="stream">Stream thats been negotiated to the specifid protocol</param>
        /// <param name="protocol">The protocol that was negotiated</param>
        /// <returns></returns>
        public static ProtocolStream<Protocol> Wrap<Protocol>(Stream stream, Protocol protocol) where Protocol : IPeerProtocol
        {
            return new ProtocolStream<Protocol>(stream, protocol);
        }
    }

    /// <summary>
    ///  Stream with a negotiated protocol
    /// </summary>
    /// <typeparam name="ProtocolType">Type of the negotiated protocol</typeparam>
    public class ProtocolStream<ProtocolType> : Stream where ProtocolType : IPeerProtocol
    {
        readonly private Stream Stream;

        /// <summary>
        /// Protocol that was negotiated
        /// </summary>
        public ProtocolType Protocol { get; private set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="protocol"></param>
        internal ProtocolStream(Stream stream, ProtocolType protocol)
        {
            Stream = stream;
            Protocol = protocol;
        }


        /// <inheritdoc />
        public override void Flush()
        {
            Stream.Flush();
        }

        /// <inheritdoc />
        public override long Seek(long offset, SeekOrigin origin)
        {
            return Stream.Seek(offset, origin);
        }

        /// <inheritdoc />
        public override void SetLength(long value)
        {
            Stream.SetLength(value);
        }

        /// <inheritdoc />
        public override int Read(byte[] buffer, int offset, int count)
        {
            return Stream.Read(buffer, offset, count);
        }

        /// <inheritdoc />
        public override void Write(byte[] buffer, int offset, int count)
        {
            Stream.Write(buffer, offset, count);
        }

        /// <inheritdoc />
        public override bool CanRead
        {
            get { return Stream.CanRead; }
        }

        /// <inheritdoc />
        public override bool CanSeek
        {
            get { return Stream.CanSeek; }
        }

        /// <inheritdoc />
        public override bool CanWrite
        {
            get { return Stream.CanWrite; }
        }

        /// <inheritdoc />
        public override long Length
        {
            get { return Stream.Length; }
        }

        /// <inheritdoc />
        public override long Position
        {
            get { return Stream.Position; }
            set { Stream.Position = value; }
        }

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            // dispose stream
            using (Stream)
            {
            }
        }
    }
}