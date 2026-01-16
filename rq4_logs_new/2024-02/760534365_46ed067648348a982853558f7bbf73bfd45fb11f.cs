using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks;
using Messaging.Buffer.Attributes;
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
    [Handler]
    public class TestHandler : HandlerBase<TestRequest>
    {
        public TestHandler(ILogger<TestHandler> logger, IMessaging messaging) : base(logger, messaging)
        {
        }

        public override void Handle(string correlationId, TestRequest request)
        {
            throw new NotImplementedException();
        }
    }

    public class SubscribeHandlers_Should : MessagingTestBase
    {
        public SubscribeHandlers_Should() : base()
        {

        }

        [Fact]
        public async void SubscribeAutomaticallyAllHandlers()
        {
            // Arrange
            var channel = $"Request:*:{typeof(TestRequest)}";
            var _loggerHandlerMock = new Mock<ILogger<TestHandler>>();

            TestHandler handlerFoundFromServiceProvider = new TestHandler(_loggerHandlerMock.Object, _service);

            _serviceProviderMock.Setup(x => x.GetService(typeof(TestHandler))).Returns(handlerFoundFromServiceProvider);
            _redisCollectionMock.Setup(x => x.SubscribeAsync(RedisChannel.Pattern(channel), _service.OnRequest)).Verifiable();


            // Act
            await _service.SubscribeHandlers();

            // Assert
            Assert.Single(_service.RequestDelegateCollection);
            Assert.Equal(handlerFoundFromServiceProvider.Handle, _service.RequestDelegateCollection[$"{typeof(TestRequest)}"]); // Verify the handler has been registered as request handler
            _redisCollectionMock.Verify(); // verify redis subscription has been made.
        }
    }
}