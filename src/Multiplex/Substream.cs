﻿using Ipfs;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace PeerTalk.Multiplex
{
    /// <summary>
    ///   A duplex substream used by the <see cref="Muxer"/>.
    /// </summary>
    /// <remarks>
    ///   Reading of data waits on the Muxer calling <see cref="AddData(byte[])"/>.
    ///   <see cref="NoMoreData"/> is used to signal the end of stream.
    ///   <para>
    ///   Writing data is buffered until <see cref="FlushAsync(CancellationToken)"/> is
    ///   called.
    ///   </para>
    /// </remarks>
    public class Substream : Stream
    {
        BufferBlock<byte[]> inBlocks = new BufferBlock<byte[]>();
        byte[] inBlock;
        int inBlockOffset;
        bool eos;

        Stream outStream = new MemoryStream();

        /// <summary>
        ///   The stream identifier.
        /// </summary>
        internal SubstreamId Id { get; private set; }
        
        /// <summary>
        ///   A name for the stream.
        /// </summary>
        /// <value>
        ///   Names do not need to be unique.
        /// </value>
        internal string Name { get; set; }

        /// <summary>
        ///   The multiplexor associated with the substream.
        /// </summary>
        private Muxer Muxer { get; set; }

        /// <inheritdoc />
        public override bool CanRead => !eos;

        /// <inheritdoc />
        public override bool CanSeek => false;

        /// <inheritdoc />
        public override bool CanWrite => outStream != null;

        /// <inheritdoc />
        public override bool CanTimeout => false;

        /// <inheritdoc />
        public override long Length => throw new NotSupportedException();

        /// <inheritdoc />
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        internal Substream(Muxer muxer, SubstreamId id, string name = "")
        {
            Muxer = muxer;
            Id = id;
            Name = name;
        }

        /// <inheritdoc />
        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        ///   Add some data that should be read by the stream.
        /// </summary>
        /// <param name="data">
        ///   The data to be read.
        /// </param>
        /// <remarks>
        ///   <b>AddData</b> is called when the muxer receives a packet for this
        ///   stream.
        /// </remarks>
        internal void AddData(byte[] data)
        {
            inBlocks.Post(data);
        }

        /// <summary>
        ///   Indicates that the stream will not receive any more data.
        /// </summary>
        /// <seealso cref="AddData(byte[])"/>
        /// <remarks>
        ///   <b>NoMoreData</b> is called when the muxer receives a packet to
        ///   close this stream.
        /// </remarks>
        public void NoMoreData()
        {
            inBlocks.Complete();
        }

        /// <inheritdoc />
        public override int Read(byte[] buffer, int offset, int count)
        {
#pragma warning disable VSTHRD002 
            return ReadAsync(buffer, offset, count).GetAwaiter().GetResult();
#pragma warning restore VSTHRD002 
        }

        /// <inheritdoc />
        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            int total = 0;
            while (count > 0 && !eos)
            {
                // Does the current block have some unread data?
                if (inBlock != null && inBlockOffset < inBlock.Length)
                {
                    var n = Math.Min(inBlock.Length - inBlockOffset, count);
                    Array.Copy(inBlock, inBlockOffset, buffer, offset, n);
                    total += n;
                    count -= n;
                    offset += n;
                    inBlockOffset += n;
                }
                // Otherwise, wait for a new block of data.
                else
                {
                    try
                    {
                        inBlock = await inBlocks.ReceiveAsync(cancellationToken).ConfigureAwait(false);
                        inBlockOffset = 0;
                    }
                    catch (InvalidOperationException) // no more data!
                    {
                        eos = true;
                    }
                }
            }
            return total;
        }
        
        /// <inheritdoc />
        public override void Flush()
        {
 #pragma warning disable VSTHRD002
            FlushAsync().GetAwaiter().GetResult();
#pragma warning restore VSTHRD002 
        }

        /// <inheritdoc />
        public override async Task FlushAsync(CancellationToken cancel)
        {
            if (outStream == null) throw new ObjectDisposedException(nameof(Substream));
            if (outStream.Length == 0)
                return;

            // Send the response over the muxer channel
            using (await Muxer.AcquireWriteAccessAsync().ConfigureAwait(false))
            {
                outStream.Position = 0;
                var header = new Header
                {
                    StreamId = Id.Id,
                    PacketType = Id.Initiator ? PacketType.MessageInitiator : PacketType.MessageReceiver
                };
                await header.WriteAsync(Muxer.Channel, cancel).ConfigureAwait(false);
                await Varint.WriteVarintAsync(Muxer.Channel, outStream.Length, cancel).ConfigureAwait(false);
                await outStream.CopyToAsync(Muxer.Channel).ConfigureAwait(false);
                await Muxer.Channel.FlushAsync(cancel).ConfigureAwait(false);

                outStream.SetLength(0);
            }
        }

        /// <inheritdoc />
        public override void Write(byte[] buffer, int offset, int count)
        {
            if (outStream == null) throw new ObjectDisposedException(nameof(Substream));
            outStream.Write(buffer, offset, count);
        }

        /// <inheritdoc />
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            if (outStream == null) throw new ObjectDisposedException(nameof(Substream));
            return outStream.WriteAsync(buffer, offset, count, cancellationToken);
        }

        /// <inheritdoc />
        public override void WriteByte(byte value)
        {
            outStream.WriteByte(value);
        }

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                Muxer?.StreamDisposed(this);

                eos = true;
                if (outStream != null)
                {
                    outStream.Dispose();
                    outStream = null;
                }
            }
            base.Dispose(disposing);
        }
    }

}

