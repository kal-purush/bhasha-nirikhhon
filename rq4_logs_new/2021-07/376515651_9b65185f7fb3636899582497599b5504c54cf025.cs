using CarService.AppCore.Models.Events;
using CarService.AppCore.Services;
using Microsoft.Extensions.Configuration;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace CarService.AppCore.Tests
{
    public class RedisEventPublisherTest
    {
        private readonly RedisEventPublisher _eventPublisher;

        public RedisEventPublisherTest()
        {
            var configuration = BuildConfigurationForEventPublisher();
            _eventPublisher = new RedisEventPublisher(configuration);
        }

        private IConfiguration BuildConfigurationForEventPublisher()
        {
            var settings = @"{""ConnectionStrings"" : {""Redis"" : ""localhost:6379""}}";
            var builder = new ConfigurationBuilder();
            builder.AddJsonStream(new MemoryStream(Encoding.UTF8.GetBytes(settings)));

            return builder.Build();
        }

        [Fact]
        public async Task PublishEvent_Success()
        {
            // Arrange
            const string CHANNEL_NAME = "repairOrders";
            const string EVENT_NAME = "test event";
            var @event = new BaseEvent<object> { Type = EVENT_NAME };

            // Act
            var clientsListenedEvent = await _eventPublisher.PublishEvent(CHANNEL_NAME, @event);

            // Assert
            Assert.True(clientsListenedEvent != 0);
        }
    }
}