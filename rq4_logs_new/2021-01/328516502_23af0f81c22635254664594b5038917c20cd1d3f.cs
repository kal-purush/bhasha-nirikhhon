using Moq;
using NUnit.Framework;
using PiLed.Devices.Config;
using PiLed.Devices.Implementations;
using PiLed.Display;
using PiLed.Models;
using System.Threading;

namespace PiLed.Test.Display
{
    public class ThrobColorDisplayTests
    {
        Mock<IPixelDevice> _pixelDeviceMock;
        PixelConfig _config;
        [SetUp]
        public void Setup()
        {
            _pixelDeviceMock = new Mock<IPixelDevice>();
            _config = new PixelConfig()
            {
                FlushRate = 10,
                NumLeds = 1
            };
        }

        [Test]
        public void ThrobColorDispalyTests_Happy()
        {
            //var buffer = new PixelBuffer();
            //buffer.PixelIndices = new int[1] { 0 };
            //buffer.Color = new PixelColor(0, 1, 1);

            //_pixelDeviceMock.SetupGet(x => x._config).Returns(_config);
            //_pixelDeviceMock.Setup(x => x.FlushColorToLeds(
            //    It.Is<PixelBuffer[]>(y => y.Length == 1
            //        && y[0].PixelIndices.Length == 1
            //        && y[0].PixelIndices[0] == 0
            //        && y[0].Color.Hue == 0
            //        && y[0].Color.Saturation == 1
            //        && y[0].Color.Value == 1)));

            //var display = new ThrobColorDisplay(_pixelDeviceMock.Object, buffer.Color);

            //display.Start(CancellationToken.None);

            //_pixelDeviceMock.VerifyAll();
        }
    }
}