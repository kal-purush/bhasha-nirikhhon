using Hippologamus.Server.Components;
using Hippologamus.Server.Factorys;
using Xunit;

namespace Hippologamus.Server.Test.Factory
{
    public class DeletePerfLogItemFactoryTest
    {
        [Fact]
        public void Create_DialogTitle()
        {
            //arrange
            //act
            var value = DeletePerfLogItemFactory.Create("42");
            //assert
            Assert.Equal("Delete Performance Log", value.DialogTitle);
        }

        [Theory]
        [InlineData("1", "Do you want to delete 1, then type 1.")]
        [InlineData("42", "Do you want to delete 42, then type 42.")]
        [InlineData("item", "Do you want to delete item, then type item.")]
        public void Create_DeleteCaption(string deleteValue, string expected)
        {
            //arrange
            //act
            var value = DeletePerfLogItemFactory.Create(deleteValue);
            //assert
            Assert.Equal(expected, value.DeleteCaption);
        }

        [Theory]
        [InlineData("1")]
        [InlineData("42")]
        [InlineData("item")]
        public void Create_CheckValue(string deleteValue)
        {
            //arrange
            //act
            var value = DeletePerfLogItemFactory.Create(deleteValue);
            //assert
            Assert.Equal(deleteValue, value.CheckValue);
        }
    }
}