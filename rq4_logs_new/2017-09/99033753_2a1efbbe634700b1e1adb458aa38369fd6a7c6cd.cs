using Microsoft.VisualStudio.TestTools.UnitTesting;
using toofz.NecroDancer.Data;

namespace toofz.NecroDancer.Tests.Data
{
    class BouncerTests
    {
        [TestClass]
        public class Constructor
        {
            [TestMethod]
            public void ReturnsInstance()
            {
                // Arrange -> Act
                var bouncer = new Bouncer();

                // Assert
                Assert.IsInstanceOfType(bouncer, typeof(Bouncer));
            }
        }

        [TestClass]
        public class MinProperty
        {
            [TestMethod]
            public void GetSetBehavior()
            {
                // Arrange
                var bouncer = new Bouncer();
                var min = 2.4;

                // Act
                bouncer.Min = min;
                var min2 = bouncer.Min;

                // Assert
                Assert.AreEqual(min, min2);
            }
        }

        [TestClass]
        public class MaxProperty
        {
            [TestMethod]
            public void GetSetBehavior()
            {
                // Arrange
                var bouncer = new Bouncer();
                var max = 2.4;

                // Act
                bouncer.Max = max;
                var max2 = bouncer.Max;

                // Assert
                Assert.AreEqual(max, max2);
            }
        }

        [TestClass]
        public class PowerProperty
        {
            [TestMethod]
            public void GetSetBehavior()
            {
                // Arrange
                var bouncer = new Bouncer();
                var power = 2.4;

                // Act
                bouncer.Power = power;
                var power2 = bouncer.Power;

                // Assert
                Assert.AreEqual(power, power2);
            }
        }

        [TestClass]
        public class StepsProperty
        {
            [TestMethod]
            public void GetSetBehavior()
            {
                // Arrange
                var bouncer = new Bouncer();
                var steps = 15;

                // Act
                bouncer.Steps = steps;
                var steps2 = bouncer.Steps;

                // Assert
                Assert.AreEqual(steps, steps2);
            }
        }
    }
}