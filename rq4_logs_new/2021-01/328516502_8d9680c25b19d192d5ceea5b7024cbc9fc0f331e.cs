using NUnit.Framework;
using PiLed.Models;

namespace PiLed.Test.Models
{
    public class PixelColorTests
    {
        [Test]
        [TestCase(0,1,1, 255,0,0, Description = "Converts Red HSV to RGB")]
        [TestCase(120, 1, 1, 0, 255, 0, Description = "Converts Green HSV to RGB")]
        [TestCase(240, 1, 1, 0, 0, 255, Description = "Converts Blue HSV to RGB")]
        public void PixelColor_HappyPaths(int h, int s, int v, int r, int g, int b)
        {
            var color = new PixelColor(h, s, v);
            (int actualR, int actualG, int actualB) = color.ConvertToRGBTuple();
            Assert.AreEqual(r, actualR);
            Assert.AreEqual(g, actualG);
            Assert.AreEqual(b, actualB);
        }
    }
}