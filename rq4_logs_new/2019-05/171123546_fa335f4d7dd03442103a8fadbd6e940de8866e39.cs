using NUnit.Framework;

namespace GenericList
{
    public class GenericList
    {
        private List<int> list1 = new List<int>() { 4, 8, 34, -7, 14, -23, 87, 2345, -143, -364, 78 };
        private List<int> emptyList = new List<int>();

        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public void OneAddTest()
        {
            list1.Add(10000);
            Assert.IsTrue(list1.Contains(10000));
        }

        [Test]
        public void ManyAddTest()
        {
            var toAdd = new int[] { 53, 589, 93, 234, 456, 4562, 98, -88, -49, -8839 };
            for (int j = 0; j < toAdd.Length; ++j)
            {
                list1.Add(toAdd[j]);
            }
            var expected = new int[] { 4, 8, 34, -7, 14, -23, 87, 2345, -143,
                -364, 78, 53, 589, 93, 234, 456, 4562, 98, -88, -49, -8839 };

            int i = 0;
            foreach (var key in list1)
            {
                Assert.AreEqual(expected[i], key);
                ++i;
            }
        }

        [Test]
        public void DeleteFromEmptySet()
        {
            Assert.IsFalse(list1.Remove(2));
        }

        [Test]
        [TestCase(12)]
        [TestCase(11)]
        [TestCase(-1)]
        public void DeleteFromIncorrectPositionTest(int position)
        {
            Assert.Throws<IncorrectPositionException>(() => list1.RemoveAt(position));
        }

        [Test]
        [TestCase(10, 78)]
        [TestCase(0, 4)]
        [TestCase(1, 8)]
        [TestCase(9, -364)]
        [TestCase(2, 34)]
        [TestCase(5, -23)]
        public void DeleteFromCorrectPositionTest(int position, int deletedValue)
        {
            list1.RemoveAt(position);
            Assert.IsFalse(list1.Contains(deletedValue));
        }
    }
}