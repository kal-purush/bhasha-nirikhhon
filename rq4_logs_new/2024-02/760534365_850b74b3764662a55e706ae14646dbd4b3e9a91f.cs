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
    public class TestRequest() : RequestBase
    {
        public string key { get; set; } = "value";
    }

    public class OnRequest_Should
    {
        private Messaging _service;
        private Mock<IRedisCollection> _redisCollectionMock;
        private Mock<ILogger<Messaging>> _loggerMock;

        public OnRequest_Should()
        {
            _loggerMock = new();
            _redisCollectionMock = new();
            _service = new Messaging(_loggerMock.Object, _redisCollectionMock.Object);
        }

        [Fact]
        public void InvokeHandler_WhenFound()
        {
            // Arrange
            var requestId = Guid.NewGuid().ToString();
            var channel = $"Request:{requestId}:{nameof(TestRequest)}";
            var value = new TestRequest().ToJson();

            var handlerInvoked = false;
            var added = _service.RequestDelegateCollection.TryAdd("TestRequest", (string a, TestRequest b) =>
            {
                handlerInvoked = true;
                Assert.Equal(requestId, a);
                Assert.Equal("value", b.key);
            });

            // Act
            _service.OnRequest(RedisChannel.Pattern(channel), value);

            // Assert
            Assert.True(handlerInvoked);
        }

        [Fact]
        public void LogError_WhenHandlingFails()
        {
            // Arrange
            var requestId = Guid.NewGuid().ToString();
            var channel = $"Request:{requestId}:{nameof(TestRequest)}";
            var value = new TestRequest().ToJson();

            var handlerInvoked = false;
            var added = _service.RequestDelegateCollection.TryAdd("TestRequest", (string a, TestRequest b) =>
            {
                throw new Exception("Invoke did not work");
            });

            // Act
            _service.OnRequest(RedisChannel.Pattern(channel), value);

            // Assert
            _loggerMock.Verify(x => x.Log(LogLevel.Error,
                        It.IsAny<EventId>(),
                        It.Is<It.IsAnyType>((v, t) => v.ToString().Contains("Request could not be handled. Error when calling delegate.")),
                        null,
                        It.Is<Func<It.IsAnyType, Exception, string>>((v, t) => true)), Times.Once);
            Assert.False(handlerInvoked);
        }
    }
}