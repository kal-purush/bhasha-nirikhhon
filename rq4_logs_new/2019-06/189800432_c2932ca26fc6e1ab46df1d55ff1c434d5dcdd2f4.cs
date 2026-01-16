using Xunit;
using OpenCode.StringExtension;

namespace OpenCode.Tests
{
    public class RemoveTrailingZeroUnitTest
    {
        [Theory]
        [InlineData("2")]
        [InlineData("2.000")]
        [InlineData("02")]
        public void ShouldReturn2(string input)
        {
            var result = input.RemoveTrailingZero();

            Assert.Equal("2", result);
        }

        [Theory]
        [InlineData("2.01")]
        [InlineData("2.0100")]
        [InlineData("02.010")]
        public void ShouldReturn201(string input)
        {
            var result = input.RemoveTrailingZero();

            Assert.Equal("2.01", result);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("test")]
        [InlineData("02v.0")]
        [InlineData("2")]
        public void ShouldReturnIuputAsIs(string input)
        {
            var result = input.RemoveTrailingZero();

            Assert.Equal(input, result);
        }
    }
}