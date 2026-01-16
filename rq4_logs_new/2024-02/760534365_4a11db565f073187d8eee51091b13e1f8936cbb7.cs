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
    public class PublishRequestAsync_Should : MessagingTestBase
    {
        public PublishRequestAsync_Should() : base()
        {

        }

        [Fact]
        public async void PublishMessage()
        {
            // Arrange
            var requestId = Guid.NewGuid().ToString();
            var channel = $"Request:{requestId}:{typeof(TestRequest)}";
            var value = new TestRequest();
            var expectedMessageSent = value.ToJson();
            _subscriberMock.Setup(x => x.PublishAsync(RedisChannel.Pattern(channel), expectedMessageSent, CommandFlags.None)).ReturnsAsync(1).Verifiable();

            // Act
            var result = await _service.PublishRequestAsync(requestId, value);

            // Assert
            Assert.Equal(1, result);
            _subscriberMock.Verify();
        }

        [Fact]
        public async void LogWargning_WhenNoReceiver()
        {
            // Arrange
            var requestId = Guid.NewGuid().ToString();
            var channel = $"Request:{requestId}:{typeof(TestRequest)}";
            var value = new TestRequest();
            var expectedMessageSent = value.ToJson();
            _subscriberMock.Setup(x => x.PublishAsync(RedisChannel.Pattern(channel), expectedMessageSent, CommandFlags.None)).ReturnsAsync(0).Verifiable();

            // Act
            var result = await _service.PublishRequestAsync(requestId, value);

            // Assert
            Assert.Equal(0, result);
            _subscriberMock.Verify();

            _loggerMock.Verify(x => x.Log(LogLevel.Warning,
                        It.IsAny<EventId>(),
                        It.Is<It.IsAnyType>((v, t) => v.ToString().Contains($"Request of type {typeof(TestRequest)} {requestId} lost.")),
                        It.IsAny<Exception>(),
                        It.Is<Func<It.IsAnyType, Exception, string>>((v, t) => true)), Times.Once);
        }

        [Fact]
        public async void LogError_WhenPublishFails()
        {
            // Arrange
            var requestId = Guid.NewGuid().ToString();
            var channel = $"Request:{requestId}:{typeof(TestRequest)}";
            var value = new TestRequest();
            var expectedMessageSent = value.ToJson();
            _subscriberMock.Setup(x => x.PublishAsync(RedisChannel.Pattern(channel), expectedMessageSent, CommandFlags.None)).Throws(new Exception("publish failed")).Verifiable();

            // Act
            var result = await _service.PublishRequestAsync(requestId, value);

            // Assert
            Assert.Equal(0, result);
            _subscriberMock.Verify();

            _loggerMock.Verify(x => x.Log(LogLevel.Error,
                        It.IsAny<EventId>(),
                        It.Is<It.IsAnyType>((v, t) => v.ToString().Contains($"Could not Publish {typeof(TestRequest)} to channel {channel}")),
                        It.IsAny<Exception>(),
                        It.Is<Func<It.IsAnyType, Exception, string>>((v, t) => true)), Times.Once);
        }
    }
}