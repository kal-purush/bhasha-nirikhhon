using LeetCodeTraining.RemoveElement;

namespace LeetCodeTraining.Tests
{
    public class RemoveElementTests
    {
        [Theory]
        [MemberData(nameof(Data))]
        public void NumberOfRemainingElementsTest(int[] nums, int val, int expectedResult)
        {
            //Act
            int result = RemoveElementSolution.RemoveElement(nums, val);

            //Assert
            Assert.Equal(expectedResult, result);
        }

        [Theory]
        [MemberData(nameof(Data))]
        public void RemainingValuesAreValidTest(int[] nums, int val, int expectedResult)
        {
            //Act
            int result = RemoveElementSolution.RemoveElement(nums, val);

            //Assert
            for (int i = 0; i < result; i++)
            {
                if (nums[i] == val)
                {
                    Assert.Fail("First elements contain values that were supposed to be removed.");
                }    
            }

            Assert.Equal(expectedResult, result);
        }

        public static IEnumerable<object[]> Data =>
            new List<object[]> ()
            {
                new object[]
                {
                    new int[] { 3, 2, 2, 3 },
                    3, 
                    2
                },
                new object[]
                {
                    new int[] { 0, 1, 2, 2, 3, 0, 4, 2 },
                    2, 
                    5
                },
                new object[]
                {
                    new int[] { 1 }, 
                    1, 
                    0
                },
                new object[]
                {
                    new int[] { 5 }, 
                    1, 
                    1
                },
                new object[]
                {
                    new int[] { 1, 1, 1, 1, 2 }, 
                    1, 
                    1
                },
                new object[]
                {
                    new int[] { 2, 2, 2 }, 
                    2, 
                    0
                }
            };
    }
}