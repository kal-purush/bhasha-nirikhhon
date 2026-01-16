using Xunit;
using NoCO2.Util;
using NoCO2.Function;

namespace NoCO2.Test
{
  public class HttpRequestDataExtensionsTest
  {
    [Fact]
    public void TryParseJson_ValidJson_ParsesSuccessfully()
    {
      // Arrange
      var json = "{\"UserKey\":\"pGIWAl55j3XH4LFHbXgsdtoM46j2\"}";

      // Arrange
      var stream = GenerateStreamFromString(json);

      // Act
      stream.TryParseJson<CreateUserBody>(out var result);

      // Assert
      Assert.Equal("pGIWAl55j3XH4LFHbXgsdtoM46j2", result.UserKey);
    }

    [Theory]
    [InlineData("")]
    [InlineData("{\"reply\":\"Invalid")]
    public void TryParseJson_ThrowsArgumentException(string jsonValue)
    {
      // Arrange
      var json = jsonValue;
      var stream = GenerateStreamFromString(json);

      // Act & Assert
      Assert.Throws<ArgumentException>(() => stream.TryParseJson<CreateUserBody>(out _));
    }

    // Helper method to convert a string to a stream
    private static Stream GenerateStreamFromString(string input)
    {
      var stream = new MemoryStream();
      var writer = new StreamWriter(stream);
      writer.Write(input);
      writer.Flush();
      stream.Position = 0;
      return stream;
    }
  }
}