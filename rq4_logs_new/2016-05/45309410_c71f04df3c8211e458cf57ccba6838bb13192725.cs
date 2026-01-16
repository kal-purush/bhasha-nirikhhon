using ListNamespace;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ListTests
{
    [TestClass]
    public class ListTest
    {
        private List<int> list;

        [TestInitialize]
        public void Initialize()
        {
            list = new List<int>();
        }

        [TestMethod]
        public void EnumeratorTest()
        {
            for (int i = 1; i < 10; ++i)
            {
                list.Add(i);
            }

            int j = 1;
            foreach (int element in list)
            {
                Assert.AreEqual(element, j);
                ++j;
            }
        }


        [TestMethod]
        public void AddTest()
        {
            for (int i = 1; i < 10; i *= 2)
            {
                list.Add(i);
            }

            Assert.AreEqual(1, list.GetValue(0));
            Assert.AreEqual(2, list.GetValue(1));
            Assert.AreEqual(8, list.GetValue(3));
        }

        [TestMethod]
        public void RemoveTest()
        {
            for (int i = 0; i < 10; ++i)
            {
                list.Add(i);
            }

            list.Remove(4);
            Assert.AreEqual(5, list.GetValue(4));

            list.Remove(0);
            Assert.AreEqual(1, list.GetValue(0));
        }

        [TestMethod()]
        [ExpectedException(typeof(EmptyListException))]
        public void EmptyListExceptionTest()
        {
            list.Print();
        }

        [TestMethod()]
        [ExpectedException(typeof(EmptyListException))]
        public void RemoveFromEmptyListTest()
        {
            list.Remove(5);
        }

        [TestMethod()]
        [ExpectedException(typeof(NonExistentItemException))]
        public void RemoveNonExistentItemTest()
        {
            for (int i = 0; i < 10; ++i)
            {
                list.Add(i + 5);
            }

            list.Remove(3);
        }

        [TestMethod()]
        [ExpectedException(typeof(NonExistentItemException))]
        public void NonExistentItemException()
        {
            list.Add(2);
            list.Add(4);
            var value = list.GetValue(10);
        }

        [TestMethod]
        public void ContainsTest()
        {
            for (int i = 0; i < 10; ++i)
            {
                list.Add(i);
            }

            for (int i = 9; i >= 0; --i)
            {
                Assert.IsTrue(list.Contains(i));
            }

            Assert.IsFalse(list.Contains(12));
        }
    }
}