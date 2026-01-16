using Xunit;

namespace OpenCode.Tests
{
    public partial class StringExtension
    {
        #region ParseDecimal
        [Theory]
        [InlineData(null)]
        [InlineData(" ")]
        [InlineData("0")]
        [InlineData("1")]
        [InlineData("-1")]
        [InlineData("abcd!@#$1234")]
        public void ParseToBool_ShouldReturnDefaultFalse(string input)
        {
            var result = input.ParseToBool();

            Assert.False(result);
        }

        [Theory]
        [InlineData(null)]
        [InlineData(" ")]
        [InlineData("0")]
        [InlineData("1")]
        [InlineData("-1")]
        [InlineData("abcd!@#$1234")]
        public void ParseToBool_ShouldReturnDefaultTrue(string input)
        {
            var result = input.ParseToBool(true);

            Assert.True(result);
        }

        [Theory]
        [InlineData("True")]
        [InlineData("False")]
        [InlineData("true")]
        [InlineData("false")]
        public void ParseToBool_ShouldParseInput(string input)
        {
            var result = input.ParseToBool();

            Assert.Equal(bool.Parse(input), result);
        }

        #endregion
    }
}