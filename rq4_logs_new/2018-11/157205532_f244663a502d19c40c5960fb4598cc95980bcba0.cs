using System;
using NUnit.Framework;
// ReSharper disable ObjectCreationAsStatement

namespace HelloWorld.Tests
{
  [TestFixture]
  public class StockMessengerTests
  {
    [Test]
    public void Constructor_GivenNullMessage_ShouldThrowException()
    {
      // Arrange
      // Act
      // Assert
      Assert.Throws<ArgumentNullException>(() => new StockMessenger(null));
    }

    [Test]
    public void GetMessage_GivenMessage_ShouldReturnMessage()
    {
      // Arrange
      const string expected = "Yeeeeeah, booooooooooooooy!";

      var messenger = new StockMessenger(expected);

      // Act
      string actual = messenger.GetMessage();

      // Assert
      Assert.AreEqual(expected, actual);
    }
  }
}