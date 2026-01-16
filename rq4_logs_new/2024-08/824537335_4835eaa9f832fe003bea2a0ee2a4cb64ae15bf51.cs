using FluentAssertions;
using POCTomlHandling.Libraries;
using Tomlyn;
using Tomlyn.Model;

namespace POCTomlHandling.Tests;

public class TomlynTests
{
    [Fact]
    public void ParseTomlString()
    {
        // Arrange
        var toml =
            @"
        [example]
        key = ""value""
        ";
        var parser = new TomlynParser();

        // Act
        var result = parser.ParseTomlString(toml, "example", "key");

        // Assert
        result.Should().Be("value");
    }
}