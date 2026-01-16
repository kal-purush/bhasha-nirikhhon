using LeetCodeTraining.SortColors;

namespace LeetCodeTraining.Tests
{
    public class SortColorsTests
    {
        [Theory]
        [MemberData(nameof(Data))]
        public void SortColorsTest(int[] nums, int[] expectedResult)
        {
            //Act
            SortColorsSolution.SortColors(nums);

            //Assert
            Assert.Equal(nums, expectedResult);
        }

        public static IEnumerable<object[]> Data =>
            new List<object[]>
            {
                new object[]
                {
                    new int[] { 2, 0, 2, 1, 1, 0 },
                    new int[] { 0, 0, 1, 1, 2, 2 }
                },
                new object[]
                {
                    new int[] { 2, 0, 1 },
                    new int[] { 0, 1, 2 }
                },
                new object[]
                {
                    new int[] { 0, 2 },
                    new int[] { 0, 2 }
                },
                new object[]
                {
                    new int[] { 2, 0, 2, 1, 1, 0, 2, 1, 2, 0, 1, 2, 0, 2, 2, 2, 0 },
                    new int[] { 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2 }
                }
            };
    }
}