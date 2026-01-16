using Moq;
using NUnit.Framework;
using PiLed.Devices.Config;
using PiLed.Devices.Implementations;
using PiLed.Display;
using PiLed.Models;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace PiLed.Test.Display
{
    class RainbowColorDisplayTests
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


        //TODO we need to do these tests a different way. They are NOT performant.
        [Test]
        public void RainbowLedPattern_Happy()
        {

            _pixelDeviceMock.SetupGet(x => x._config).Returns(_config);
            _pixelDeviceMock.Setup(x => x.FlushColorToLeds(It.IsAny<PixelBuffer>()));

            var display = new RainbowColorDisplay(_pixelDeviceMock.Object, RainbowColorDisplay.RainbowType.Led);

            var cs = new CancellationTokenSource();
            Task.Run(() => display.Start(cs.Token));


            Thread.Sleep(5);
            cs.Cancel();
            _pixelDeviceMock.VerifyAll();
            for (double i = 0; i < 360; i = i + 50)
            {
                _pixelDeviceMock.Verify(x => x.FlushColorToLeds(It.Is<PixelBuffer>(y => y.Color.Hue == i)), times: Times.AtLeastOnce);
            }
            _pixelDeviceMock.Verify(x => x.FlushColorToLeds(It.Is<PixelBuffer>(x => x.Color.Saturation != 1 || x.Color.Value != 1)), times: Times.Never);
        }

        [Test]
        public void RainbowColorWheelPattern_Happy()
        {

            _pixelDeviceMock.SetupGet(x => x._config).Returns(_config);
            _pixelDeviceMock.Setup(x => x.FlushColorToLeds(It.IsAny<PixelBuffer>()));

            var display = new RainbowColorDisplay(_pixelDeviceMock.Object, RainbowColorDisplay.RainbowType.ColorWheel);

            var cs = new CancellationTokenSource();
            Task.Run(() => display.Start(cs.Token));


            Thread.Sleep(3);
            cs.Cancel();
            _pixelDeviceMock.VerifyAll();
            
            for(double i = 0; i < 360; i = i + 50)
            {
                _pixelDeviceMock.Verify(x => x.FlushColorToLeds(It.Is<PixelBuffer>(y => y.Color.Hue == i)), times: Times.AtLeastOnce);
            }
            _pixelDeviceMock.Verify(x => x.FlushColorToLeds(It.Is<PixelBuffer>(y => y.Color.Saturation != 1 || y.Color.Value != 1)), times: Times.Never);
        }
    }
}