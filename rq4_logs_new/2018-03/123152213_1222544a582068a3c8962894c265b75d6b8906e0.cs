using Xunit;
using System.Collections.Generic;
using System.Messaging;


namespace ReturnToSourceQueueTests
{
    public class MsmqUtilitiesTests
    {
        [Fact]
        public void SaveMessageHeadersSavesNewHeaders()
        {
            var message = new Message();
            var newHeaders = new Dictionary<string, string>
            {
                {"key1", "value1"},
                {"key2", "value2"}
            };

            MsmqUtilities.SaveMessageHeaders(newHeaders, message);

            Assert.True(message.Extension.Length == 325);

            var deserializedHeaders = MsmqUtilities.DeserializeMessageHeaders(message);

            Assert.True(deserializedHeaders.Count == 2);

            deserializedHeaders.TryGetValue("key1", out var value);
            Assert.Equal("value1", value);

            deserializedHeaders.TryGetValue("key2", out value);
            Assert.Equal("value2", value);
        }
    }
}