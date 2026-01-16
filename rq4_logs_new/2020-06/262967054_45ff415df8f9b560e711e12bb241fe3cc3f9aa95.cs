using System;

namespace CameraBasler.Entities
{
    public class BrightnessDistributionSnapshotData
    {
        public byte[] Image { get; set; }

        public double ExposureTime { get; set; }

        public string PixelFormat { get; set; }

        public int Energy { get; set; }

        public DateTime DateTime { get; set; }
    }
}