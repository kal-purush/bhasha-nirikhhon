using System;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Mathmagician.Tests
{
    [TestClass]
    public class EvenTest
    {
        [TestMethod]
        public void Even0()
        {
            Assert.IsNotNull(new Even(1));
        }
        [TestMethod]
        public void Even1()
        {
            Assert.IsInstanceOfType(new Even(1).EvenList(), typeof(List<int>));
        }
        [TestMethod]
        public void Even2()
        {
            Even num = new Even(5);
            Assert.IsTrue(5 == num.EvenList().Count);
        }
        [TestMethod]
        public void Even3()
        {
            Even num = new Even(1);
            Assert.IsTrue(2 == num.EvenList()[0]);
        }
        [TestMethod]
        public void Even4()
        {
            Even num = new Even(5);
            Assert.IsTrue(10 == num.EvenList()[4]);
        }
    }
}