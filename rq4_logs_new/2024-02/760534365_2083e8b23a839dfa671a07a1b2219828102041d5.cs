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
    public class OnResponseShould : MessagingTestBase
    {
        public OnResponseShould() : base()
        {

        }

        [Fact]
        public void InvokeHandler_WhenFound()
        {
            // Arrange
            var requestId = Guid.NewGuid().ToString();
            var channel = $"Response:{requestId}";
            var value = new TestResponse(requestId).ToJson();

            var handlerInvoked = false;
            var added = _service.ResponseDelegateCollection.TryAdd(requestId, (Messaging a, ReceivedEventArgs b) =>
            {
                handlerInvoked = true;
                Assert.NotNull(a);
                Assert.Equal(channel, b.Channel);
                Assert.Equal(value, b.Value);
                Assert.Equal(requestId, b.CorrelationId);
            });

            // Act
            _service.OnResponse(RedisChannel.Pattern(channel), value);

            // Assert
            Assert.True(handlerInvoked);
        }

        [Fact]
        public void LogError_WhenHandlingFails()
        {
            // Arrange
            var requestId = Guid.NewGuid().ToString();
            var channel = $"Response:{requestId}";
            var value = new TestResponse(requestId).ToJson();

            var added = _service.ResponseDelegateCollection.TryAdd(requestId, (Messaging a, ReceivedEventArgs b) =>
            {
                throw new Exception("Handling failed.");
            });

            // Act
            _service.OnResponse(RedisChannel.Pattern(channel), value);

            // Assert
            _loggerMock.Verify(x => x.Log(LogLevel.Error,
                        It.IsAny<EventId>(),
                        It.Is<It.IsAnyType>((v, t) => v.ToString().Contains("Response could not be handled. Error when calling delegate.")),
                        It.IsAny<Exception>(),
                        It.Is<Func<It.IsAnyType, Exception, string>>((v, t) => true)), Times.Once);
        }

        [Fact]
        public void LogError_WhenNoHandlerFound()
        {
            // Arrange
            var requestId = Guid.NewGuid().ToString();
            var channel = $"Request:{requestId}";
            var value = new TestRequest().ToJson();

            var handlerInvoked = false;

            // Act
            _service.OnResponse(RedisChannel.Pattern(channel), value);

            // Assert
            _loggerMock.Verify(x => x.Log(LogLevel.Error,
                        It.IsAny<EventId>(),
                        It.Is<It.IsAnyType>((v, t) => v.ToString().Contains("No Respons delegate found. Could not handle the response.")),
                        It.IsAny<Exception>(),
                        It.Is<Func<It.IsAnyType, Exception, string>>((v, t) => true)), Times.Once);
            Assert.False(handlerInvoked);
        }
    }
}