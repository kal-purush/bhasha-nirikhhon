namespace Sprig.Core.Tests;

using Sprig.Core.Messages;
using Xunit.Sdk;

public class MessageSerializerTests
{
    [Fact]
    public void SerializeHandshakeRequest()
    {
        var req = new HandshakeRequest(123);
        var serialized = Serializer.Serialize(req);
        Assert.True(serialized.Any(b => b != 0), "Serialized bytes should not be all zeros");
    }

    [Fact]
    public void SerializeHandshakeResponse()
    {
        var res = HandshakeResponse.Accept(123);
        var serialized = Serializer.Serialize(res);
        Assert.True(serialized.Any(b => b != 0), "Serialized bytes should not be all zeros");
        Assert.Contains(serialized, b => b == 123);
    }
}