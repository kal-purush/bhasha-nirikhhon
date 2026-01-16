using NUnit.Framework;

namespace Filter.Tests_NUnit
{
    /// <summary>
    /// This class contains tests for DigitFilterFunc
    /// </summary>
    [TestFixture]
    public class FilterTest
    {
        /// <summary>
        /// Tests for FilterDigitFunc
        /// </summary>
        [TestCase(new int[] { 7, 1, 2, 3, 4, 5, 6, 7, 68, 69, 70, 15, 17 }, new int[] { 7, 70, 17 }, 7)]
        [TestCase(new int[] { 7, 1, 2, 3, 4, 5, 6, 7, 68, 69, 70, 15, 17 }, new int[] { 6, 68, 69 }, 6)]
        [TestCase(new int[] { 7, 1, 2, 35, 4, 5, 63, 7, 68, 69, 70, 15, 17, 55, 22, 55, 55 }, new int[] { 35, 5, 15, 55 }, 5)]
        [TestCase(new int[] { 7, 1, 2, 3, 4, 5, 6, 7, 68, 69, 71, 15, 17 }, new int[] {}, 0)]
        [TestCase(new int[] {}, new int[] {}, 1)]
        public void FilterDigitFuncTests(int[] initialArray, int[] expectedArray, int digit)
        {
            int[] result = FilterDigit.FilterDigitFunc(initialArray, digit);

            for (int i = 0; i < expectedArray.Length; i++)
            {
                if (result[i] != expectedArray[i])
                {
                    Assert.Fail();
                }
            }
        }
    }
}