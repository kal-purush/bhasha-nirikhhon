using LeetCodeTraining.RegexMatching;

namespace LeetCodeTraining.Tests
{
    public class RegexMatchingTests
    {
        [Theory]
        [InlineData("aa", "a", false)]
        [InlineData("aa", "a*", true)]
        [InlineData("ab", ".*", true)]
        [InlineData("aaaa", "aaa", false)]
        [InlineData("abcdef", "abcdef", true)]
        [InlineData("ab", ".*..", true)]
        [InlineData("ab", "ac*b*a*", true)]
        [InlineData("ab", "c*b*a*b*a*", true)]
        [InlineData("ab", "a*b*", true)]
        [InlineData("ab", "dct", false)]
        [InlineData("b", "aaaa.", false)]
        [InlineData("abcd", "ab.d", true)]
        [InlineData("abed", "ab.d", true)]
        public void IsMatchTests(string s, string p, bool expectedResult)
        {
            //Act
            bool result = RegexMatchingSolution.IsMatch(s, p);

            //Assert
            Assert.Equal(expectedResult, result);
        }
    }
}