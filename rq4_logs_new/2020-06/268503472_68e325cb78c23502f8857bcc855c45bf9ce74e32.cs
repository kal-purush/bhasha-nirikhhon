using System.Linq;
using NUnit.Framework;

namespace SquareSequenceAnalyzer.Tests
{
    public class EnumerableExtensionTest
    {
        [TestCase(new double[]{1, 2, 3}, new double[]{1, 4, 9})]
        [TestCase(new double[]{5, 6, 12}, new double[]{25, 36, 144})]
        [TestCase(new double[]{-1, -2, -3}, new double[]{1, 4, 9})]
        public void FirstListSquaredEqualSecondList_ShouldReturn_True(double[] baseNumbers, double[] squaredNumbers)
        {
            Assert.True(squaredNumbers.ToList().ArePowerOf(baseNumbers, 2));
        }

        [TestCase(new double[] { 1, 2, 3 }, new double[] { 1, 8, 27 })]
        [TestCase(new double[] { 5, 6, 12 }, new double[] { 125, 216, 1728 })]
        [TestCase(new double[] { -1, -2, -3 }, new double[] { -1, -8, -27 })]
        public void FirstListToPowerOfThreeEqualSecondList_ShouldReturn_True(double[] baseNumbers, double[] compoundedNumbers)
        {
            Assert.True(compoundedNumbers.ToList().ArePowerOf(baseNumbers, 3));
        }

        [TestCase(new double[] { 1, 2, 3 }, new double[] { 1, 1, 1 })]
        [TestCase(new double[] { 1, 2, 3 }, new double[] { 1, 4, 2 })]
        [TestCase(new double[] { 1, 2, 3 }, new double[] { 1, 2, 9 })]
        [TestCase(new double[] { 5, 6, 12 }, new double[] { -25, 36, 144 })]
        [TestCase(new double[] { -1, -2, -3 }, new double[] { 2, 4, -9 })]
        public void FirstListSquaredEqualSecondList_ShouldReturn_False(double[] baseNumbers, double[] squaredNumbers)
        {
            Assert.False(squaredNumbers.ToList().ArePowerOf(baseNumbers, 2));
        }

        [TestCase(new double[] { 1, 2, 3 }, new double[] { 1, -8, -27 })]
        [TestCase(new double[] { 5, 6, 12 }, new double[] { -125, -216, -1728 })]
        [TestCase(new double[] { -1, -2, -3 }, new double[] { 1, 8, 27 })]
        public void FirstListToPowerOfThreeEqualSecondList_ShouldReturn_False(double[] baseNumbers, double[] compoundedNumbers)
        {
            Assert.False(compoundedNumbers.ToList().ArePowerOf(baseNumbers, 3));
        }

        [TestCase(new string[] {"1", "2", "3"}, new double[] {1, 2, 3})]
        [TestCase(new string[] {"5", "6", "12" }, new double[] {5, 6, 12 })]
        [TestCase(new string[] { "-1", "-2", "-3" }, new double[] { -1, -2, -3 })]
        public void FirstLisOfStringsConvertedToDoubles_ShouldReturn_SecondList(string[] stringNumbers, double[] resultNumbers)
        {
            Assert.AreEqual(resultNumbers, stringNumbers.ToList().ToDoubles());
        }
    }
}