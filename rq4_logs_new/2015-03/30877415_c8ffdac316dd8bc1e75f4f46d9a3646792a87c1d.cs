namespace List.Tests
{
    using System;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using ListNamespace;

    [TestClass]
    public class ListTest
    {
        private List list;

        [TestInitialize]
        public void Initialize()
        {
            list = new List();
        }

        [TestMethod]
        public void AddTest()
        {
            list.AddInOrder(1);
            Assert.IsFalse(list.IsEmpty());
        }

        [TestMethod]
        public void ExistsTest()
        {
            list.AddInOrder(1);
            Assert.IsTrue(list.Exists(1));
            Assert.IsFalse(list.Exists(2));
        }

        [TestMethod]
        public void AddThreeElementsTest()
        {
            list.AddInOrder(2);
            list.AddInOrder(1);
            list.AddInOrder(3);
            list.PrintList();
            Assert.AreEqual(1, list.ReturnValue(0));
            Assert.AreEqual(2, list.ReturnValue(1));
            Assert.AreEqual(3, list.ReturnValue(2));
        }

        [TestMethod]
        public void RemoveElementTest()
        {
            list.AddInOrder(6);
            list.RemoveElement(6);
            Assert.IsFalse(list.Exists(6));
        }
    }
}