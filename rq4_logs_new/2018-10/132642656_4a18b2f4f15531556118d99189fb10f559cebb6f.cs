using Xunit;
using BggCoreSdk.Dto;

namespace BggCoreSdk.UnitTests.Dto
{
    public class BoardGameCommentDtoUnitTests
    {
        [Fact]
        public void ToString_Returns_Value()
        {
            const string VALUE = "this is a comment";

            // arrange
            var dto = new BoardGameCommentDto()
            {
                Value = VALUE
            };

            // act
            var result = dto.ToString();

            // assert
            Assert.Equal(VALUE, result);
        }
    }
}