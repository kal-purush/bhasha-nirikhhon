using System;
using Xamarin.Forms;

namespace MapBox
{
    public sealed class PinImageDescriptor
    {
        public Color Color { get; private set; }
        public string FileName { get; private set; }
        public string FolderName { get; private set; }
        public PinImageDescriptorType Type { get; private set; }
        public string FilePath { get; private set; }
        public double Width { get; private set; }
        public double Height { get; private set; }

        private PinImageDescriptor()
        {
        }

        internal static PinImageDescriptor FromBundle(string fileName)
        {
            return new PinImageDescriptor()
            {
                FileName = fileName,
                Type = PinImageDescriptorType.Bundle
            };
        }

        internal static PinImageDescriptor FromPath(string folderPath, string fileName, double height, double width)
        {
            return new PinImageDescriptor()
            {
                FolderName = folderPath,
                FileName = fileName,
                Type = PinImageDescriptorType.Path,
                FilePath = folderPath + "." + fileName,
                Height = height,
                Width = width
            };
        }
    }

    public enum PinImageDescriptorType
    {
        Bundle,
        Path
    }
}