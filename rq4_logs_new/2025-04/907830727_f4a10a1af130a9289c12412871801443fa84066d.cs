using EasyTravel.Application.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Moq;
using NUnit.Framework;
using System;
using System.Text;

namespace EasyTravel.Test.ApplicationTests.Services
{
    [TestFixture]
    public class SessionServiceTests
    {
        private Mock<IHttpContextAccessor> _httpContextAccessorMock = null!;
        private Mock<ISession> _sessionMock = null!;
        private Mock<ILogger<SessionService>> _loggerMock = null!;
        private SessionService _sessionService = null!;

        [SetUp]
        public void SetUp()
        {
            _httpContextAccessorMock = new Mock<IHttpContextAccessor>();
            _sessionMock = new Mock<ISession>();
            _loggerMock = new Mock<ILogger<SessionService>>();

            var httpContextMock = new Mock<HttpContext>();
            httpContextMock.Setup(c => c.Session).Returns(_sessionMock.Object);

            _httpContextAccessorMock.Setup(a => a.HttpContext).Returns(httpContextMock.Object);

            _sessionService = new SessionService(_httpContextAccessorMock.Object, _loggerMock.Object);
        }

        [Test]
        public void SetString_ShouldThrowArgumentException_WhenKeyIsNullOrEmpty()
        {
            // Act & Assert
            var ex = Assert.Throws<ArgumentException>(() => _sessionService.SetString(null!, "value"));
            Assert.That(ex!.ParamName, Is.EqualTo("key"));

            ex = Assert.Throws<ArgumentException>(() => _sessionService.SetString(string.Empty, "value"));
            Assert.That(ex!.ParamName, Is.EqualTo("key"));

            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Warning,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Attempted to set a session value with an empty or null key.")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Exactly(2));
        }

        [Test]
        public void SetString_ShouldThrowArgumentNullException_WhenValueIsNull()
        {
            // Act & Assert
            var ex = Assert.Throws<ArgumentNullException>(() => _sessionService.SetString("key", null!));
            Assert.That(ex!.ParamName, Is.EqualTo("value"));

            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Warning,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Attempted to set a session value with a null value for key: key")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Test]
        public void SetString_ShouldSetSessionValue_WhenKeyAndValueAreValid()
        {
            // Arrange
            var key = "key";
            var value = "value";
            var bytes = Encoding.UTF8.GetBytes(value);

            // Act
            _sessionService.SetString(key, value);

            // Assert
            _sessionMock.Verify(s => s.Set(key, bytes), Times.Once);
            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains($"Successfully set session value for key: {key}")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Test]
        public void GetString_ShouldThrowArgumentException_WhenKeyIsNullOrEmpty()
        {
            // Act & Assert
            var ex = Assert.Throws<ArgumentException>(() => _sessionService.GetString(null!));
            Assert.That(ex!.ParamName, Is.EqualTo("key"));

            ex = Assert.Throws<ArgumentException>(() => _sessionService.GetString(string.Empty));
            Assert.That(ex!.ParamName, Is.EqualTo("key"));

            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Warning,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Attempted to get a session value with an empty or null key.")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Exactly(2));
        }

        [Test]
        public void GetString_ShouldReturnEmptyString_WhenKeyDoesNotExist()
        {
            // Arrange
            var key = "key";
            _sessionMock.Setup(s => s.TryGetValue(key, out It.Ref<byte[]?>.IsAny)).Returns(false);

            // Act
            var result = _sessionService.GetString(key);

            // Assert
            Assert.That(result, Is.EqualTo(string.Empty));
            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains($"No session value found for key: {key}")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Test]
        public void GetString_ShouldReturnSessionValue_WhenKeyExists()
        {
            // Arrange
            var key = "key";
            var value = "value";
            var bytes = Encoding.UTF8.GetBytes(value);
            _sessionMock.Setup(s => s.TryGetValue(key, out bytes)).Returns(true);

            // Act
            var result = _sessionService.GetString(key);

            // Assert
            Assert.That(result, Is.EqualTo(value));
            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains($"Successfully retrieved session value for key: {key}")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Test]
        public void Remove_ShouldThrowArgumentException_WhenKeyIsNullOrEmpty()
        {
            // Act & Assert
            var ex = Assert.Throws<ArgumentException>(() => _sessionService.Remove(null!));
            Assert.That(ex!.ParamName, Is.EqualTo("key"));

            ex = Assert.Throws<ArgumentException>(() => _sessionService.Remove(string.Empty));
            Assert.That(ex!.ParamName, Is.EqualTo("key"));

            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Warning,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Attempted to remove a session value with an empty or null key.")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Exactly(2));
        }

        [Test]
        public void Remove_ShouldRemoveSessionValue_WhenKeyIsValid()
        {
            // Arrange
            var key = "key";

            // Act
            _sessionService.Remove(key);

            // Assert
            _sessionMock.Verify(s => s.Remove(key), Times.Once);
            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains($"Successfully removed session value for key: {key}")),
                    null,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }
    }
}
