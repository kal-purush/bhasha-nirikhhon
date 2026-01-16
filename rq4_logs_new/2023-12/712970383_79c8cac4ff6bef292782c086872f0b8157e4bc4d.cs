using LeetCodeTraining.PrintInOrder;

namespace LeetCodeTraining.Tests
{
    public class PrintInOrderTests
    {
        [Theory]
        [InlineData(new int[] { 1, 2, 3 })]
        [InlineData(new int[] { 1, 3, 2 })]
        [InlineData(new int[] { 2, 1, 3 })]
        [InlineData(new int[] { 2, 3, 1 })]
        [InlineData(new int[] { 3, 1, 2 })]
        [InlineData(new int[] { 3, 2, 1 })]
        public void PrintInOrderTest(int[] nums)
        {
            //Arrange
            string expectedResult = "firstsecondthird";

            //Act
            string result = PrintInOrderSolution.PrintInOrder(nums);

            //Assert
            Assert.Equal(expectedResult, result);
        }
    }
}