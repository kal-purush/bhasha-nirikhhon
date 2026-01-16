using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace GzipApplication.ChunkedFIleWriter
{
    public abstract class BaseChunkedWriter : IChunkedDataWriter, IDisposable
    {
        private readonly ConcurrentBag<OrderedChunk> _chunks;
        private readonly Func<long?> _getChunksCount;

        private long _chunksWritten = 0;

        private readonly SortedDictionary<long, OrderedChunk> _sortedDictionary =
            new SortedDictionary<long, OrderedChunk>();

        protected BaseChunkedWriter(ConcurrentBag<OrderedChunk> chunks, Func<long?> getChunksCount)
        {
            _chunks = chunks;
            _getChunksCount = getChunksCount;
        }


        public bool FlushReadyChunks()
        {
            var compressedChunksCount = _chunks.Count;
            var count = 0;

            while (count < compressedChunksCount && _chunks.TryTake(out OrderedChunk orderedChunk))
            {
                _sortedDictionary.Add(orderedChunk.Order, orderedChunk);
                count++;
            }

            while (_sortedDictionary.ContainsKey(_chunksWritten))
            {
                var orderedChunk = _sortedDictionary[_chunksWritten];

                Write(orderedChunk);

                _sortedDictionary.Remove(_chunksWritten);
                _chunksWritten++;
            }

            Flush();

            var chunksCount = _getChunksCount();

            if (_chunksWritten == chunksCount)
            {
                return false;
            }

            return true;
        }

        protected abstract void Write(OrderedChunk chunk);

        protected abstract void Flush();

        public abstract void Dispose();
    }
}