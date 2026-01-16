using NUnit.Framework;

namespace Summator.Tests
{
    public class Tests
    {
        [Test]
       public void Test_Sum_TwoPositiveNumbers()
        {
            int result = Summator.Sum(new int[] {5,7});
            Assert.That(result == 12);
        }
        [Test]
        public void Test_Sum_OnePositiveNumber()
        {
            int result = Summator.Sum(new int[] { 5});
            Assert.That(result == 5);
        }
        [Test]
        public void Test_Sum_TwoNegativeNumbers()
        {
            int result = Summator.Sum(new int[] {-9,-7});
            Assert.That(result == -16);
        }
        [Test]
        public void Test_Sum_EmptyArray()
        {
            int result = Summator.Sum(new int[] {5 });
            Assert.That(result == 5);
        }
        [Test]
        public void Test_Sum_BigValues()
        {
            int actual = Summator.Sum(new int[] {200,200,200 });
            Assert.AreEqual(actual, 600);
        }
    }
}