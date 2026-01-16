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
    public class UnsubscribeRequestAsync_Should : MessagingTestBase
    {
        public UnsubscribeRequestAsync_Should() : base()
        {

        }

        [Fact]
        public async void UnsubscribeFromRequestChannel()
        {
            // Arrange
            var channel = $"Request:*:{typeof(TestRequest)}";
            _redisCollectionMock.Setup(x => x.UnsubscribeAsync(RedisChannel.Pattern(channel))).Verifiable();
            _service.RequestDelegateCollection.TryAdd($"{typeof(TestRequest)}", () => { });

            // Act
            await _service.UnsubscribeRequestAsync<TestRequest>();

            // Assert
            _redisCollectionMock.Verify();
            Assert.Empty(_service.RequestDelegateCollection);
        }

        [Fact]
        public async void LogError_WhenUnsubFails()
        {
            // Arrange
            var channel = $"Request:*:{typeof(TestRequest)}";
            _redisCollectionMock.Setup(x => x.UnsubscribeAsync(RedisChannel.Pattern(channel))).Throws(new Exception("Unsub fails.")).Verifiable();
            _service.RequestDelegateCollection.TryAdd($"{typeof(TestRequest)}", () => { });

            // Act
            await _service.UnsubscribeRequestAsync<TestRequest>();

            // Assert
            _redisCollectionMock.Verify();
            Assert.NotEmpty(_service.RequestDelegateCollection);

            _loggerMock.Verify(x => x.Log(LogLevel.Error,
                        It.IsAny<EventId>(),
                        It.Is<It.IsAnyType>((v, t) => v.ToString().Contains($"Could not Unsubscribe to channel {channel}")),
                        It.IsAny<Exception>(),
                        It.Is<Func<It.IsAnyType, Exception, string>>((v, t) => true)), Times.Once);
        }
    }
}