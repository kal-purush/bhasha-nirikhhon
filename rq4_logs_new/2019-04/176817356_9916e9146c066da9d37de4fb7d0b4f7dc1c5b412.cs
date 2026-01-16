using System;
using Xunit;
using multibracketvalidation;

namespace XUnitTestProject1
{
    public class UnitTest1
    {
        [Theory]
        [InlineData("([{}])")]
        [InlineData("()[]")]
        public void TestValidateTrue(string test)
        {
            Assert.True(Program.Validate(test));
        }

        [Theory]
        [InlineData("[(])")]
        [InlineData("][")]
        [InlineData("([]}")]
        public void TestValidateFalse(string test)
        {
            Assert.False(Program.Validate(test));
        }
    }
}