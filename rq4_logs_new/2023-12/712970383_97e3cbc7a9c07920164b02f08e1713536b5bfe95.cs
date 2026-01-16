using LeetCodeTraining.ValidParentheses;

namespace LeetCodeTraining.Tests
{
    public class ValidParenthesesTests
    {
        [Theory]
        [InlineData("()", true)]
        [InlineData("()[]{}", true)]
        [InlineData("(]", false)]
        [InlineData("({)}", false)]
        [InlineData("({}[]())", true)]
        [InlineData("{{{}}}(())[[]]", true)]
        [InlineData("(()))", false)]
        [InlineData("((())", false)]
        [InlineData("(((((", false)]
        [InlineData("))))", false)]
        public void IsValid(string input, bool expectedResult)
        {
            //Act
            bool result = ValidParenthesesSolution.IsValid(input);

            //Assert
            Assert.Equal(expectedResult, result);
        }
    }
}