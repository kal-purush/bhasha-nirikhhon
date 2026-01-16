using Moq;
using NUnit.Framework;
using PiLed.Devices.Config;
using PiLed.Devices.Implementations;
using PiLed.Display;
using PiLed.Models;

namespace PiLed.Test.Display
{
    public class SolidColorDisplayTests
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
        public void WS2801ConvertBufferToBytes_Happy()
        {
            var buffer = new PixelBuffer();
            buffer.PixelIndices = new int[1] { 0 };
            buffer.Color = new PixelColor(0, 1, 1);

            _pixelDeviceMock.SetupGet(x => x._config.NumLeds).Returns(_config.NumLeds);
            _pixelDeviceMock.Setup(x => x.FlushColorToLeds(buffer));

            var display = new SolidColorDisplay(_pixelDeviceMock.Object, buffer.Color);

            _pixelDeviceMock.VerifyAll();
        }
    }
}