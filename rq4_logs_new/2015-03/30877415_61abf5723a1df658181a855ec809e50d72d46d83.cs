using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ListNamespace;
using FunctionsNamespace;

namespace Tests
{
    /// <summary>
    /// Tests for functions: Map, Filter, Fold
    /// </summary>
    [TestClass]
    public class FunctionsTests
    {
        private List<int> list;

        [TestInitialize]
        public void Initialize()
        {
            list = new List<int>();
        }

        [TestMethod]
        public void MapTest()
        {
            list.AddTail(2);
            list.AddTail(3);
            list = MapClass.Map(list, x => x * 2);
            Assert.AreEqual(6, list.ReturnValue(1));
            Assert.AreEqual(4, list.ReturnValue(0));
        }

        [TestMethod]
        public void FilterTest()
        {
            list.AddTail(3);
            list = FilterClass.Filter(list, x => (x % 2 == 0));
            Assert.IsTrue(list.Size == 0);
        }

        [TestMethod]
        public void FoldTest()
        {
            list.AddTail(1);
            list.AddTail(2);
            list.AddTail(3);
            Assert.AreEqual(6, FoldClass.Fold(list, 1, (acc, elem) => acc * elem));
        }
    }
}