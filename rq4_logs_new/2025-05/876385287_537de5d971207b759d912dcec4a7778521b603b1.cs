using LangSharp.Core.Commands;
using LangSharp.Core.Enums;

namespace LangSharp.UnitTests.Core.Commands
{
    public class CommandRequestTests
    {
        [Fact]
        public void Constructor_ShouldInitializePropertiesCorrectly()
        {
            // Arrange
            var commandType = TypeCommand.ExecuteDatabaseQuery;
            var parameter = "SELECT * FROM Users";

            // Act
            var commandRequest = new CommandRequest(commandType, parameter);

            // Assert
            Assert.Equal(commandType, commandRequest.CommandType);
            Assert.Equal(parameter, commandRequest.Parameter);
        }
    }
}