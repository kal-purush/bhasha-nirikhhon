using System;
using System.IO;
using GzipApplication.Constants;
using GzipApplication.Data;
using GzipApplication.Exceptions.User;

namespace GzipApplication.ChunkedFileReader
{
    public abstract class ChunkedFileReader : IChunkedFileReader
    {
        public abstract bool HasMore { get; }
        public abstract long? LengthInChunks { get; }
        
        protected long ChunksRead = 0;
        
        public OrderedChunk ReadChunk()
        {
            if (!HasMore)
            {
                throw new InvalidOperationException("All chunks were already read.");
            }

            var readBytes = ReadBytes();

            return new OrderedChunk
            {
                Data = readBytes,
                Order = ChunksRead++
            };
        }

        public ChunkedFileReader(FileStream fileStream)
        {
            ValidateFileStream(fileStream);
        }

        private void ValidateFileStream(FileStream stream)
        {
            if (!stream.CanRead)
            {
                throw new UnreadableFileException(string.Format(UserMessages.UnreadableFileFormat, stream.Name));
            }

            if (stream.Position != 0)
            {
                throw new InvalidArgumentException("Other thread already tried to read a file!");
            }
        }

        protected abstract byte[] ReadBytes();
    }
}