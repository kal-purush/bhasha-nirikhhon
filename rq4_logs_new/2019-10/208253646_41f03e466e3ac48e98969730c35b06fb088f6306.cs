using Xunit;

namespace TheFipster.Munchkin.GameEvents.UnitTest
{
    public class InventoryTest
    {
        [Fact]
        public void GetAllMessageTypesInAssembly_ResultsInNotEmptyMessageTypeList_Test()
        {
            // Arrange
            var inventory = new Inventory();

            // Act
            var types = inventory.GetMessageTypes();

            // Assert
            Assert.NotEmpty(types);
        }

        [Fact]
        public void GetAllActionTypesInAssembly_ResultsInNotEmptyMessageTypeList_Test()
        {
            // Arrange
            var inventory = new Inventory();

            // Act
            var types = inventory.GetActionTypes();

            // Assert
            Assert.NotEmpty(types);
        }
    }
}