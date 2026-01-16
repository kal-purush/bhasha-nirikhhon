using Xunit;
using OpenCode.StringExtension;

namespace OpenCode.Tests.StringExtension
{
    public partial class StringExtension
    {
        #region ParseDecimal
        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("abcd!@#$")]
        public void ParseToDecimal_ShouldReturnDefault0(string input)
        {
            var result = input.ParseToDecimal();

            Assert.Equal(0, result);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("abcd!@#$1234")]
        public void ParseToDecimal_ShouldReturnDefault5(string input)
        {
            var result = input.ParseToDecimal(5);

            Assert.Equal(5, result);
        }

        [Theory]
        [InlineData("1")]
        [InlineData("1.0")]
        [InlineData("01.0")]
        public void ParseToDecimal_ShouldParseInput(string input)
        {
            var result = input.ParseToDecimal(5);

            Assert.Equal(decimal.Parse(input), result);
        }

        #endregion
    }
}