using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Messaging.Buffer.Buffer;
using Microsoft.Extensions.Logging;
using Moq;

namespace Messaging.Buffer.Test.BufferTest
{
    public class TestRequest : RequestBase
    {

    }

    public class TestResponse : ResponseBase
    {
        public string data;
        public TestResponse(string correlationId) : base(correlationId)
        {
        }
    }

    public class TestRequestBuffer : RequestBufferBase<TestRequest, TestResponse>
    {
        public TestRequestBuffer(IMessaging messaging, TestRequest request, ILogger<RequestBufferBase<TestRequest, TestResponse>> logger) : base(messaging, request, logger)
        {
        }

        /// <summary>
        /// Concatenate all response data.
        /// </summary>
        /// <returns></returns>
        protected override TestResponse Aggregate()
        {
            var result = new TestResponse(CorrelationId);
            ResponseCollection.ForEach(response =>
            {
                result.data += response.data;
            });
            return result;
        }
    }

    public class SendRequestAsync_Should
    {
        private Mock<IMessaging> _messaging;
        private Mock<ILogger<TestRequestBuffer>> _logger;
        private TestRequestBuffer buffer;

        public SendRequestAsync_Should()
        {
            _messaging = new();
            _logger = new();
            buffer = new TestRequestBuffer(_messaging.Object, new TestRequest(), _logger.Object);
        }

        /// <summary>
        /// Send a TestRequest and verify the content of TestResponse.
        /// The content of TestResponse is computed in Aggregate method.
        /// </summary>
        [Fact]
        public async void ProduceAggregatedResponse()
        {
            // Arrange
            _messaging.Setup(x => x.SubscribeResponseAsync(buffer.CorrelationId, buffer.OnResponse)).Verifiable();
            _messaging.Setup(x => x.PublishRequestAsync(buffer.CorrelationId, buffer.Request)).ReturnsAsync(2).Callback(async () =>
            {
                await Task.Delay(500);
                buffer.OnResponse(null, new ReceivedEventArgs()
                {
                    Channel = $"Response:{buffer.CorrelationId}",
                    CorrelationId = buffer.CorrelationId,
                    MessageType = $"{typeof(TestResponse)}",
                    Value = new TestResponse(buffer.CorrelationId)
                    {
                        data = "ResponseONE"
                    }.ToJson()
                });
                buffer.OnResponse(null, new ReceivedEventArgs()
                {
                    Channel = $"Response:{buffer.CorrelationId}",
                    CorrelationId = buffer.CorrelationId,
                    MessageType = $"{typeof(TestResponse)}",
                    Value = new TestResponse(buffer.CorrelationId)
                    {
                        data = "ResponseTWO"
                    }.ToJson()
                });
            }).Verifiable();
            _messaging.Setup(x => x.UnsubscribeResponseAsync(buffer.CorrelationId)).Verifiable();

            // Act
            var result = await buffer.SendRequestAsync();

            // Assert
            Assert.Equal("ResponseONEResponseTWO", result.data);
            _messaging.Verify();
        }
    }
}