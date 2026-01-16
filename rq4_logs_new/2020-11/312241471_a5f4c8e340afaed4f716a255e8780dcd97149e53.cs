using BuddyDomain.Battle.ValueObjects.Actor;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace TestDomain.Battle.ValueObjects.Actor
{
    [TestClass]
    public class TestSpeed
    {
        [TestMethod]
        public void Compare()
        {
            var faster = new Speed(51);
            var slower = new Speed(50);
            Assert.IsTrue(faster > slower);
            Assert.IsFalse(slower > faster);
            Assert.IsFalse(faster < slower);
            Assert.IsTrue(slower < faster);
        }
    }
}