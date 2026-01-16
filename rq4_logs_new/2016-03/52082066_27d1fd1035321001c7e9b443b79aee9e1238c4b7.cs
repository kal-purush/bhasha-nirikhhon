using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ListNamespace;

namespace ListTest
{
    [TestClass]
    public class ListTest
    {
        private List tmp;

        [TestInitialize]
        public void Inizialaze()
        {
            tmp = new List();
        }

        [TestMethod]
        public void PushPopTest()
        {
            tmp.Push(1);
            Assert.AreEqual(1, tmp.Pop(1));
            tmp.Pop(1);
            Assert.AreEqual(0, tmp.Pop(1));
        }

        [TestMethod]
        public void FindTest()
        {
            tmp.Push(1);
            tmp.Push(232);
            tmp.Push(1);
            tmp.Push(3);
            tmp.Push(4);
            tmp.Push(554);
            tmp.Push(12);
            tmp.Push(1);
            Assert.AreEqual(true, tmp.Find(554));
            tmp.Push(23);
        }

        [TestMethod]
        public void InIndexAndREmoveAtTEst()
        {
            tmp.InIndex(1, 2);
            tmp.InIndex(0, 1);
            Assert.AreEqual(2, tmp.Pop(2));
            tmp.InIndex(2, 3);
            tmp.RemomeAt(2);
            Assert.AreEqual(3, tmp.Pop(3));
        }
    }
}