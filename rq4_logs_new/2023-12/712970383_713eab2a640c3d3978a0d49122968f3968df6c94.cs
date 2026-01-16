using LeetCodeTraining.NextPermutation;

namespace LeetCodeTraining.Tests
{
    public class NextPermutationTests
    {
        [Theory]
        [InlineData(new int[] { 1, 2, 3 }, new int[] { 1, 3, 2 })]
        [InlineData(new int[] { 3, 2, 1 }, new int[] { 1, 2, 3 })]
        [InlineData(new int[] { 1, 1, 5 }, new int[] { 1, 5, 1 })]
        [InlineData(new int[] { 1, 4, 2, 5, 3 }, new int[] { 1, 4, 3, 2, 5 })]
        [InlineData(new int[] { 2, 3, 3, 1 }, new int[] { 3, 1, 2, 3 })]
        [InlineData(new int[] { 1 }, new int[] { 1 })]
        [InlineData(new int[] { 1, 2 }, new int[] { 2, 1 })]
        [InlineData(new int[] { 5, 1, 1, 1, 1 }, new int[] { 1, 1, 1, 1, 5 })]
        [InlineData(new int[] { 5, 4, 3, 2, 1 }, new int[] { 1, 2, 3, 4, 5 })]
        [InlineData(new int[] { 3, 3, 3, 2, 1 }, new int[] { 1, 2, 3, 3, 3 })]
        [InlineData(new int[] { 1, 8, 7, 4, 3, 2, 1 }, new int[] { 2, 1, 1, 3, 4, 7, 8 })]
        public void NextPermutationTest(int[] nums, int[] expectedResult)
        {
            //Act
            NextPermutationSolution.NextPermutation(nums);

            //Assert
            Assert.Equal(nums, expectedResult);
        }
    }
}