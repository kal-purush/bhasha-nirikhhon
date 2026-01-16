using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace GzipApplication
{
    public class ChunkedDataWriter: IDisposable
    {
        private readonly ConcurrentBag<OrderedChunk> _compressedChunks;
        private readonly long _chunksCount;
        private readonly FileStream _fileStream;
        private long _chunksWritten = 0;
        
        public ChunkedDataWriter(string outputFilename, ConcurrentBag<OrderedChunk> compressedCompressedChunks, long chunksCount)
        {
            _compressedChunks = compressedCompressedChunks;
            _chunksCount = chunksCount;
            _fileStream = File.Create(outputFilename);
        }

        public void Dispose()
        {
            _fileStream.Dispose();
        }

        public bool Flush()
        {
            var compressedChunksCount = _compressedChunks.Count;

            var sortedDictionary = new SortedDictionary<long, OrderedChunk>();
            
            while (sortedDictionary.Count < compressedChunksCount && _compressedChunks.TryTake(out OrderedChunk orderedChunk))
            {
                sortedDictionary.Add(orderedChunk.Order, orderedChunk);
            }

            while (sortedDictionary.ContainsKey(_chunksWritten))
            {
                var orderedChunk = sortedDictionary[_chunksWritten];
                
                _fileStream.Write(orderedChunk.Data);
                _chunksWritten++;
            }
            
            _fileStream.Flush(true);

            if (_chunksWritten == _chunksCount)
            {
                return false;
            }

            return true;
        }
    }
}