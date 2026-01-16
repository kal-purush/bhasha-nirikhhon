using NUnit.Framework;

namespace DragRace.Test
{
    public class Tests
    {
        private Car[] _cars =
        {
            new Audi("Audi"), 
            new Bmw("Bmw"), 
            new Lexus("Lexus"), 
            new Tesla("Tesla")
        };

        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public void Car_CheckGetMethods()
        {            
            Assert.AreEqual(_cars[0].GetName(), "Audi", "Name entry has not been saved correctly");
            Assert.AreEqual(_cars[0].GetSpeed(), 0, "Speed entry has not been saved correctly");
        }

        [Test]
        public void Car_CheckSpeedUpMethod()
        {
            _cars[1].SpeedUp();
            Assert.AreEqual(_cars[1].GetSpeed(), 1, "SpeedUp method does not work correctly");
            _cars[1].SpeedUp();
            _cars[1].SpeedUp();
            Assert.AreEqual(_cars[1].GetSpeed(), 3, "SpeedUp method does not work correctly");
        }

        [Test]
        public void Car_Lexus_CheckNitrousOxideMethod()
        {
            _cars[2].UseNitrousOxideEngine();
            Assert.AreEqual(_cars[2].GetSpeed(), 10, "Nitrous Oxide method does not work correctly for class Lexus");
        }

        [Test]
        public void Car_Tesla_CheckNitrousOxideMethod()
        {
            _cars[3].UseNitrousOxideEngine();
            Assert.AreEqual(_cars[3].GetSpeed(), 12, "Nitrous Oxide method does not work correctly for class Tesla");
        }

        [Test]
        public void Car_SlowDownMethod()
        {
            _cars[1].SlowDown();
            Assert.AreEqual(_cars[1].GetSpeed(), 2, "Slow down method does not work correctly");
        }

        [Test]
        public void Car_SlowDownMethodForLessThan0()
        {
            _cars[1].SlowDown();
            _cars[1].SlowDown();
            _cars[1].SlowDown();
            Assert.AreEqual(_cars[1].GetSpeed(), 0, "Slow down method does not work correctly if speed < 0");
        }
    }
}