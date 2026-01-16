using System;

namespace toofz.NecroDancer.ImageManager
{
    sealed class ImageFile
    {
        public ImageFile(string baseName, int frameIndex, string type, string ext, Guid format, byte[] data)
        {
            FrameIndex = frameIndex;
            Format = format;
            Data = data;

            Name = $"{baseName}_{frameIndex}_{type}{ext}";
        }

        public int FrameIndex { get; }
        public Guid Format { get; }
        public byte[] Data { get; }
        public string Name { get; }
    }
}