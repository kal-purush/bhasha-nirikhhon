using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using HashTable;

namespace HashTable.Test
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
            tmp.Push("ror");
            Assert.AreEqual("ror", tmp.Pop("ror"));
            tmp.Pop("ror");
            Assert.AreEqual("", tmp.Pop("ror"));
        }

        [TestMethod]
        public void FindTest()
        {
            tmp.Push("ro2");
            tmp.Push("r23");
            tmp.Push("rofe");
            tmp.Push("frgfr");
            tmp.Push("22or");
            tmp.Push("r454r");
            tmp.Push("ror");
            tmp.Push("r32");
            Assert.AreEqual(true, tmp.Find("ror"));
            tmp.Push("ror");
        }
    }
}