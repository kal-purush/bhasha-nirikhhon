using FluentAssertions;
using Utils;

namespace JiraWorkLogsTests;

[TestClass]
public class UnitTests
{
    [TestMethod]
    public void TestConstants()
    {
        // Arrange

        // Act
        var connectionString = Constants.DatabaseConnectionString;

        // Assert
        Assert.IsTrue(null != connectionString && connectionString.Length > 0);

        connectionString.Should().NotBeNullOrEmpty();
        connectionString.Should().Contain("Server");
        connectionString.Should().Contain("Database");
        connectionString.Should().Contain("UserName");
        connectionString.Should().Contain("Password");

        connectionString.Should()
            .NotBeNullOrEmpty()
            .And.ContainAll(["Server", "Database", "UserName", "Password"],
                "because it should contain all keywords");
    }
}