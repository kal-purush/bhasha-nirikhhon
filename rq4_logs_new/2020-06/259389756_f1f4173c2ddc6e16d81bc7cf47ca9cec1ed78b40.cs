using Hippologamus.DTO.DTO;
using Xunit;

namespace Hippologamus.DTO.Test.DTO
{
    public class BaseCollectionSearchTest
    {
        [Fact]
        public void Order_DefaultValue()
        {
            //arrange
            //act
            var value = new SearchClass();
            //assert
            Assert.Null(value.OrderBy);
            Assert.True(value.OrderAscending);
        }

        [Theory]
        [InlineData("AAA")]
        [InlineData("BBB")]
        [InlineData("CCC")]
        public void OrderBy_IsSet(string orderBy)
        {
            //arrange
            //act
            var value = new SearchClass() { OrderBy = orderBy };
            //assert
            Assert.Equal(orderBy, value.OrderBy);
            Assert.True(value.OrderAscending);
        }

        public class SearchClass : BaseCollectionSearch
        { }
    }
}