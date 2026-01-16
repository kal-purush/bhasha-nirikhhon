namespace Sprig.Server.Tests;
using Sprig.Client;
using Sprig.Core.Messages;

public class HandshakeTests : IntegrationBase
{
    public HandshakeTests()
        : base(8080)
    {
    }

    [Fact]
    public async Task SendHandshakeRequest_RespondsOK()
    {
        await Client.SendAsync(new HandshakeRequest(1), Cancellation);
        var response = await Client.ReceiveAsync(Cancellation);

        Assert.Equal(MessageKind.HandshakeResponse, response.Kind);
        Assert.Equal(ResponseCode.Ok, ((HandshakeResponse)response).Code);
    }
}