using System;
using System.Threading;
using GzipApplication.Data;
using GzipApplication.ReaderWriter;
using GzipApplication.WorkQueue;

namespace GzipApplication.ChunkedFileReader
{
    public class BufferedReader
    {
        private readonly IChunkedFileReader _fileReader;
        private readonly EvaluationQueue _evaluationQueue;
        private readonly Action<OrderedChunk> _onRead;
        private readonly SemaphoreSlim _readSlotsSemaphore;

        public BufferedReader(IChunkedFileReader fileReader, EvaluationQueue evaluationQueue, Action<OrderedChunk> onRead, SemaphoreSlim readSlotsSemaphore)
        {
            _fileReader = fileReader;
            _evaluationQueue = evaluationQueue;
            _onRead = onRead;
            _readSlotsSemaphore = readSlotsSemaphore;
        }

        private readonly int _bufferSize = CpuBoundWorkQueue.ParallelWorkMax * 2;
        
        public bool ReadChunks()
        {   
            int readCount = 0;

            //TODO this could lead to threads starvation
            bool BufferIsFull() => readCount >= _bufferSize;

            while (_fileReader.HasMore && !BufferIsFull() && _readSlotsSemaphore.Wait(TimeSpan.Zero))
            {
                OrderedChunk chunk = _fileReader.ReadChunk();

                readCount++;

                _onRead(chunk);
            }

            bool isCompleted = !_fileReader.HasMore;

            if (!isCompleted)
            {
                _evaluationQueue.Enqueue(new Function
                {
                    Name = nameof(ReadChunks),
                    Payload = ReadChunks
                });
            }

            return isCompleted;
        }
    }
}