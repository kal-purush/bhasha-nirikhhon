using Moq;
using NUnit.Framework;
using PiLed.Devices.Config;
using PiLed.Devices.Implementations;
using PiLed.Devices.SPI;
using PiLed.Models;
using System.Diagnostics;

namespace PiLed.Test.Device
{
    public class WS2801PixelDeviceTests
    {
        private Mock<ISpiHandler> _spiHandlerMock;
        private PixelConfig _config;

        [SetUp]
        public void Setup()
        {
            //Since almost no logic exist in the individual implementations, we can just one of the implementing classes to test the abstraction

            _spiHandlerMock = new Mock<ISpiHandler>();

            _config = new PixelConfig()
            {
                NumLeds = 1,
                FlushRate = 10
            };
        }

        [Test]
        public void WS2801ConvertBufferToBytes_Happy()
        {
            var buffer = new PixelBuffer();
            buffer.PixelIndices = new int[1] { 0 };
            buffer.Color = new PixelColor(0, 1, 1);

            var expectedByteArray = new byte[3] { 255, 0, 0 };
            _spiHandlerMock.Setup(x => x.FlushBytesToSpi(expectedByteArray));


            var device = new WS2801PixelDevice(_config, _spiHandlerMock.Object);

            device.FlushColorToLeds(buffer);

            _spiHandlerMock.VerifyAll();
        }

        [Test]
        public void WS2801ConvertBuffersToColor_Happy()
        {
            _config.NumLeds = 2;
            var buffer = new PixelBuffer();
            buffer.PixelIndices = new int[2] { 0, 1 };
            buffer.Color = new PixelColor(0, 1, 1);

            var expectedByteArray = new byte[6] { 255, 0, 0, 255,0,0 };
            _spiHandlerMock.Setup(x => x.FlushBytesToSpi(expectedByteArray));


            var device = new WS2801PixelDevice(_config, _spiHandlerMock.Object);

            device.FlushColorToLeds(buffer);

            _spiHandlerMock.VerifyAll();
        }

        [Test]
        public void WS2801ConvertBuffersToColor_InvalidPixelIndexDoesntBreak()
        {
            _config.NumLeds = 2;
            var buffer = new PixelBuffer();
            buffer.PixelIndices = new int[3] { 0, 1, 3 };
            buffer.Color = new PixelColor(0, 1, 1);

            var expectedByteArray = new byte[6] { 255, 0, 0, 255, 0, 0 };
            _spiHandlerMock.Setup(x => x.FlushBytesToSpi(expectedByteArray));


            var device = new WS2801PixelDevice(_config, _spiHandlerMock.Object);

            device.FlushColorToLeds(buffer);

            _spiHandlerMock.VerifyAll();
        }

        [Test]
        public void WS2801ConvertBuffersToColor_DifferentColorsInBuffer()
        {
            _config.NumLeds = 2;
            var bufferArray = new PixelBuffer[2];

            bufferArray[0] = new PixelBuffer();
            bufferArray[0].PixelIndices = new int[1] { 0 };
            bufferArray[0].Color = new PixelColor(0, 1, 1);

            bufferArray[1] = new PixelBuffer();
            bufferArray[1].PixelIndices = new int[1] {1 };
            bufferArray[1].Color = new PixelColor(120, 1, 1);

            var expectedByteArray = new byte[6] { 255, 0, 0, 0, 255, 0 };
            _spiHandlerMock.Setup(x => x.FlushBytesToSpi(expectedByteArray));


            var device = new WS2801PixelDevice(_config, _spiHandlerMock.Object);

            device.FlushColorToLeds(bufferArray);

            _spiHandlerMock.VerifyAll();
        }

        [Test]
        public void WS2801ConvertBuffersToColor_FlushRateHappy()
        {
            _config.FlushRate = 1000;
            var buffer = new PixelBuffer();
            buffer.PixelIndices = new int[1] { 0 };
            buffer.Color = new PixelColor(0, 1, 1);

            var expectedByteArray = new byte[3] { 255, 0, 0 };
            _spiHandlerMock.Setup(x => x.FlushBytesToSpi(expectedByteArray));


            var device = new WS2801PixelDevice(_config, _spiHandlerMock.Object);
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            device.FlushColorToLeds(buffer);
            device.FlushColorToLeds(buffer);
            device.FlushColorToLeds(buffer);
            device.FlushColorToLeds(buffer);
            var elapsedMs = stopwatch.ElapsedMilliseconds;
            Assert.Greater(elapsedMs, 3000);
            Assert.Less(elapsedMs, 3150);
            _spiHandlerMock.VerifyAll();
        }
    }
}