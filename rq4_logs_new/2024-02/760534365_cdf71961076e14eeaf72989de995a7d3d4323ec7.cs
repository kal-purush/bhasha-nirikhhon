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
    public class TestRequest : RequestBase
    {
        public string key { get; set; } = "value";
    }
    public class TestResponse : ResponseBase
    {
        public TestResponse(string correlationId) : base(correlationId)
        {
        }

        public string key { get; set; } = "value";
    }

    public class MessagingTestBase
    {
        protected Messaging _service;
        protected Mock<IRedisCollection> _redisCollectionMock;
        protected Mock<ILogger<Messaging>> _loggerMock;

        public MessagingTestBase()
        {
            _loggerMock = new();
            _redisCollectionMock = new();
            _service = new Messaging(_loggerMock.Object, _redisCollectionMock.Object);
        }
    }
}