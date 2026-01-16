using LeetCodeTraining.RemoveDuplicates;
using System.Reflection;

namespace LeetCodeTraining.Tests
{
    public class RemoveDuplicatesTests
    {
        [Theory]
        [MemberData(nameof(Data))]
        public void NumberOfUniqueElementsTest(int[] nums, int expectedResult)
        {
            //Act
            int result = RemoveDuplicatesSolution.RemoveDuplicates(nums);

            //Assert
            Assert.Equal(expectedResult, result);
        }

        [Theory]
        [MemberData(nameof(Data))]
        public void RemainingElementsAreSortedTest(int[] nums, int expectedResult)
        {
            //Arrange
            int[] uniqueNums = new int[nums.Length];

            //Act
            int result = RemoveDuplicatesSolution.RemoveDuplicates(nums);
            nums.CopyTo(uniqueNums, 0);
            Array.Sort(uniqueNums, 0, result - 1);

            //Assert
            if (result != expectedResult)
            {
                Assert.Fail("Number of unique elements differs from expected number.");
            }

            Assert.Equal(nums, uniqueNums);
        }

        [Theory]
        [MemberData(nameof(Data))]
        public void RemainingElementsAreUniqueTest(int[] nums, int expectedResult)
        {
            //Act
            int result = RemoveDuplicatesSolution.RemoveDuplicates(nums);

            //Assert
            for (int i = 0; i < result - 1; i++)
            {
                if (nums[i] == nums[i + 1])
                {
                    Assert.Fail($"Element {nums[i]} appears at least twice.");
                }
            }

            Assert.Equal(expectedResult, result);
        }



        public static IEnumerable<object[]> Data =>
            new List<object[]>()
            {
                new object[]
                {
                    new int[] { 1, 1, 2 },
                    2
                },
                new object[]
                {
                    new int[] { 0, 0, 1, 1, 1, 2, 2, 3, 3, 4 },
                    5
                },
                new object[]
                {
                    new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 },
                    10
                },
                new object[]
                {
                    new int[] { 1 },
                    1
                },
                new object[]
                {
                    new int[] { 1, 1, 1, 1, 1, 1 },
                    1
                }
            };
    }
}