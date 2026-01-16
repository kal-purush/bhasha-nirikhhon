using System;
using System.IO;
using System.Threading;
using GzipApplication.Data;

namespace GzipApplication.ChunkedWriter
{
    public class ChunkWriter : BaseChunkedWriter
    {
        private readonly Stream _stream;

        public ChunkWriter(Stream outputStream, Func<long?> getChunksCount, ManualResetEvent writeCompletedEvent) :
            base(getChunksCount, writeCompletedEvent)
        {
            _stream = outputStream;
        }

        protected override void Write(OrderedChunk chunk)
        {
            _stream.Write(chunk.Data.Span);
        }

        protected override void Flush()
        {
            _stream.Flush();
        }

        public override void Dispose()
        {
            _stream.Dispose();
        }
    }
}