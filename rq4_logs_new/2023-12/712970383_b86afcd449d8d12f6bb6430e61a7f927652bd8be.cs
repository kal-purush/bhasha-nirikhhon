using LeetCodeTraining.IndexOfSubstring;

namespace LeetCodeTraining.Tests
{
    public class IndexOfSubstringTests
    {
        [Theory]
        [InlineData("sadbutsad", "sad", 0)]
        [InlineData("leetcode", "leeto", -1)]
        [InlineData("sadbutsad", "but", 3)]
        [InlineData("aaaaabbbbb", "bbbbb", 5)]
        public void StrStrTests(string haystack, string needle, int expectedResult)
        {
            //Act
            int result = IndexOfSubstringSolution.StrStr(haystack, needle);

            //Assert
            Assert.Equal(result, expectedResult);
        }
    }
}