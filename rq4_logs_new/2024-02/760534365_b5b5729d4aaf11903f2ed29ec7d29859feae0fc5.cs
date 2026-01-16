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
using Moq;
using Newtonsoft.Json;
using StackExchange.Redis;
using Xunit.Sdk;

namespace Messaging.Buffer.Test.MessagingTest
{
    public class SubscribeAnyRequestAsync_Should : MessagingTestBase
    {
        public SubscribeAnyRequestAsync_Should() : base()
        {

        }

        [Fact]
        public async void SubscribeToGenericRequestChannel()
        {
            // Arrange
            var channel = "Request:*:*";
            _redisCollectionMock.Setup(x => x.SubscribeAsync(RedisChannel.Pattern(channel), _service.OnRequest)).Verifiable();

            // Act
            await _service.SubscribeAnyRequestAsync((object sender, ReceivedEventArgs e) => {  });

            // Assert
            _subscriberMock.Verify();
        }

        [Fact]
        public async void LogError_WhenSubscriptionFails()
        {
            // Arrange
            var channel = "Request:*:*";
            _redisCollectionMock.Setup(x => x.SubscribeAsync(RedisChannel.Pattern(channel), _service.OnRequest)).Throws(new Exception("Subscription fails.")).Verifiable();

            // Act
            await _service.SubscribeAnyRequestAsync((object sender, ReceivedEventArgs e) => { });

            // Assert
            _subscriberMock.Verify();

            _loggerMock.Verify(x => x.Log(LogLevel.Error,
                        It.IsAny<EventId>(),
                        It.Is<It.IsAnyType>((v, t) => v.ToString().Contains($"Could not Subscribe to channel {channel}")),
                        It.IsAny<Exception>(),
                        It.Is<Func<It.IsAnyType, Exception, string>>((v, t) => true)), Times.Once);
        }
    }
}