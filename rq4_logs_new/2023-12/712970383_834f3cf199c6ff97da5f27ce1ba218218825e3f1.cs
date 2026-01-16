using LeetCodeTraining.DivideIntegers;

namespace LeetCodeTraining.Tests
{
    public class DivideIntegersTests
    {
        [Theory]
        [InlineData(10, 3, 3)]
        [InlineData(7, -3, -2)]
        [InlineData(100, 1, 100)]
        [InlineData(100, -1, -100)]
        [InlineData(20, 4, 5)]
        [InlineData(-20, -4, 5)]
        [InlineData(20, -3, -6)]
        [InlineData(-20, 6, -3)]
        [InlineData(int.MaxValue, 1, int.MaxValue)]
        [InlineData(int.MinValue, -1, int.MaxValue)]
        [InlineData(int.MaxValue, -1, int.MinValue + 1)]
        [InlineData(0, 10, 0)]
        [InlineData(0, -10, 0)]
        public void DivideTests(int dividend, int divisor, int expectedResult)
        {
            //Act
            int result = DivideIntegersSolution.Divide(dividend, divisor);

            //Assert
            Assert.Equal(expectedResult, result);
        }
    }
}