using NUnit.Framework;

namespace GravityCalculator.Test
{
    public class Tests
    {
        private double _expected;

        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public void FreeFallPosition_1km10sec()
        {
            double result = GravityCalculator.Program.FreeFallPosition(1000,0,10);
            _expected = 509.5;
            Assert.AreEqual(_expected, result,0.001, "509.5 m from 1000 m after 10 sec of Free fall");
        }

        [Test]
        public void FreeFallPosition_1km10secFlyingUp()
        {
            double result = GravityCalculator.Program.FreeFallPosition(1000, -5, 10);
            _expected = 559.5;
            Assert.AreEqual(_expected, result, 0.001, "559.5 m from 1000 m after 10 sec of Free fall with initial negative velocity");
        }

        [Test]
        public void FreeFallPosition_1km60sec()
        {
            double result = GravityCalculator.Program.FreeFallPosition(1000, 0, 60);
            _expected = 0;
            Assert.AreEqual(_expected, result, 0.001, "0 m (ground level) from 1000 m after 60 sec of Free fall");
        }

        [Test]
        public void FreeFallPosition_FromGroundLevel()
        {
            double result = GravityCalculator.Program.FreeFallPosition(0, 10, 60);
            _expected = 0;
            Assert.AreEqual(_expected, result, 0.001, "0 m (ground level) from ground level");
        }
    }
}