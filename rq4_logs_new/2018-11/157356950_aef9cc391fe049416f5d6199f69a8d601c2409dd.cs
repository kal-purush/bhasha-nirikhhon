using Microsoft.VisualStudio.TestTools.UnitTesting;
using Qsec.Randomizer;

namespace QSec.Randomizer.Tests
{
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void TestMethod1()
        {
            var t = new Class1();
            var result = t.SayHello("world");
            Assert.AreEqual("Hello world", result);
        }
    }
}