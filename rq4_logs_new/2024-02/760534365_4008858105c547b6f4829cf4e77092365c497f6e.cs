using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks;
using Messaging.Buffer.Buffer;
using Messaging.Buffer.Redis;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestPlatform.CommunicationUtilities;
using Moq;
using Newtonsoft.Json;
using StackExchange.Redis;
using Xunit.Sdk;

namespace Messaging.Buffer.Test.MessagingTest
{
    public class SubscribeResponseAsync_Should : MessagingTestBase
    {
        public SubscribeResponseAsync_Should() : base()
        {

        }

        private void TestResponseHandler(object obj, ReceivedEventArgs args)
        {

        }

        [Fact]
        public async void SubscribeToResponseChannel()
        {
            // Arrange
            var requestId = Guid.NewGuid().ToString();
            var channel = $"Response:*";
            _redisCollectionMock.Setup(x => x.SubscribeAsync(RedisChannel.Pattern(channel), _service.OnResponse)).Verifiable();

            // Act
            await _service.SubscribeResponseAsync(requestId, TestResponseHandler);

            // Assert
            Assert.Single(_service.ResponseDelegateCollection);
            Assert.Equal(TestResponseHandler, _service.ResponseDelegateCollection[$"{requestId}"]);
            _subscriberMock.Verify();
        }

        [Fact]
        public async void LogError_WhenSubscriptionFails()
        {
            // Arrange
            var requestId = Guid.NewGuid().ToString();
            var channel = $"Response:{requestId}";
            _redisCollectionMock.Setup(x => x.SubscribeAsync(RedisChannel.Pattern(channel), _service.OnResponse)).Throws(new Exception("Subscription fails.")).Verifiable();

            // Act
            await _service.SubscribeResponseAsync(requestId, TestResponseHandler);

            // Assert
            _subscriberMock.Verify();
            Assert.Empty(_service.ResponseDelegateCollection);

            _loggerMock.Verify(x => x.Log(LogLevel.Error,
                        It.IsAny<EventId>(),
                        It.Is<It.IsAnyType>((v, t) => v.ToString().Contains($"Could not Subscribe to channel {channel}")),
                        It.IsAny<Exception>(),
                        It.Is<Func<It.IsAnyType, Exception, string>>((v, t) => true)), Times.Once);
        }
    }
}