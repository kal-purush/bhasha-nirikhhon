using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Messaging.Buffer.Buffer;
using Messaging.Buffer.Redis;
using Microsoft.Extensions.Logging;
using Moq;
using Newtonsoft.Json;
using StackExchange.Redis;
using Xunit.Sdk;

namespace Messaging.Buffer.Test.MessagingTest
{
    public class PublishResponseAsync_Should : MessagingTestBase
    {
        public PublishResponseAsync_Should() : base()
        {

        }

        [Fact]
        public async void PublishMessage()
        {
            // Arrange
            var requestId = Guid.NewGuid().ToString();
            var channel = $"Response:{requestId}";
            var value = new TestResponse(requestId);
            var expectedMessageSent = value.ToJson();
            _subscriberMock.Setup(x => x.PublishAsync(RedisChannel.Pattern(channel), expectedMessageSent, CommandFlags.None)).ReturnsAsync(1).Verifiable();

            // Act
            await _service.PublishResponseAsync(requestId, value);

            // Assert
            _subscriberMock.Verify();
        }

        [Fact]
        public async void LogWargning_WhenNoReceiver()
        {
            // Arrange
            var requestId = Guid.NewGuid().ToString();
            var channel = $"Response:{requestId}";
            var value = new TestResponse(requestId);
            var expectedMessageSent = value.ToJson();
            _subscriberMock.Setup(x => x.PublishAsync(RedisChannel.Pattern(channel), expectedMessageSent, CommandFlags.None)).ReturnsAsync(0).Verifiable();

            // Act
            await _service.PublishResponseAsync(requestId, value);

            // Assert
            _subscriberMock.Verify();

            _loggerMock.Verify(x => x.Log(LogLevel.Warning,
                        It.IsAny<EventId>(),
                        It.Is<It.IsAnyType>((v, t) => v.ToString().Contains($"Response for of type {typeof(TestResponse)} with correlation id {requestId} lost.")),
                        It.IsAny<Exception>(),
                        It.Is<Func<It.IsAnyType, Exception, string>>((v, t) => true)), Times.Once);
        }

        [Fact]
        public async void LogWargning_WhenMoreThanOneReceiver()
        {
            // Arrange
            var requestId = Guid.NewGuid().ToString();
            var channel = $"Response:{requestId}";
            var value = new TestResponse(requestId);
            var expectedMessageSent = value.ToJson();
            _subscriberMock.Setup(x => x.PublishAsync(RedisChannel.Pattern(channel), expectedMessageSent, CommandFlags.None)).ReturnsAsync(2).Verifiable();

            // Act
            await _service.PublishResponseAsync(requestId, value);

            // Assert
            _subscriberMock.Verify();

            _loggerMock.Verify(x => x.Log(LogLevel.Warning,
                        It.IsAny<EventId>(),
                        It.Is<It.IsAnyType>((v, t) => v.ToString().Contains($"More than one instance received the Response of type {typeof(TestResponse)} with correlation id {requestId}")),
                        It.IsAny<Exception>(),
                        It.Is<Func<It.IsAnyType, Exception, string>>((v, t) => true)), Times.Once);
        }

        [Fact]
        public async void LogError_WhenPublishFails()
        {
            // Arrange
            var requestId = Guid.NewGuid().ToString();
            var channel = $"Response:{requestId}";
            var value = new TestResponse(requestId);
            var expectedMessageSent = value.ToJson();
            _subscriberMock.Setup(x => x.PublishAsync(RedisChannel.Pattern(channel), expectedMessageSent, CommandFlags.None)).Throws(new Exception("Publish fails.")).Verifiable();

            // Act
            await _service.PublishResponseAsync(requestId, value);

            // Assert
            _subscriberMock.Verify();

            _loggerMock.Verify(x => x.Log(LogLevel.Error,
                        It.IsAny<EventId>(),
                        It.Is<It.IsAnyType>((v, t) => v.ToString().Contains($"Could not Publish response {typeof(TestResponse)} to channel {channel}")),
                        It.IsAny<Exception>(),
                        It.Is<Func<It.IsAnyType, Exception, string>>((v, t) => true)), Times.Once);
        }
    }
}