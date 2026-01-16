using ClassLibrary;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace UnitTestProject1
{
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void TestMethod1()
        {
            double price;
            ClassLibrary.Car car=new Car();

            price = car.Price();

            Assert.AreEqual(price, 240);
        }

        [TestMethod]
        public void TestMethod2()
        {
            string type;
            ClassLibrary.Car car = new Car();

            type = car.VehicleType();

            Assert.AreEqual(type, "Car");
        }

    }
}