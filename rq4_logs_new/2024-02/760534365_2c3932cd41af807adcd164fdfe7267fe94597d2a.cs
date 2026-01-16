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
    public class UnsubscribeAnyRequestAsync_Should : MessagingTestBase
    {
        public UnsubscribeAnyRequestAsync_Should() : base()
        {

        }

        [Fact]
        public async void UnsubscribeFromGenericRequestChannel()
        {
            // Arrange
            var channel = "Request:*:*";
            _redisCollectionMock.Setup(x => x.UnsubscribeAsync(RedisChannel.Pattern(channel))).Verifiable();

            // Act
            await _service.UnsubscribeAnyRequestAsync();

            // Assert
            _redisCollectionMock.Verify();
        }

        [Fact]
        public async void LogError_WhenUnsubFails()
        {
            // Arrange
            var channel = "Request:*:*";
            _redisCollectionMock.Setup(x => x.UnsubscribeAsync(RedisChannel.Pattern(channel))).Throws(new Exception("Unsub fails.")).Verifiable();

            // Act
            await _service.UnsubscribeAnyRequestAsync();

            // Assert
            _redisCollectionMock.Verify();

            _loggerMock.Verify(x => x.Log(LogLevel.Error,
                        It.IsAny<EventId>(),
                        It.Is<It.IsAnyType>((v, t) => v.ToString().Contains($"Could not Unsubscribe to channel {channel}")),
                        It.IsAny<Exception>(),
                        It.Is<Func<It.IsAnyType, Exception, string>>((v, t) => true)), Times.Once);
        }
    }
}