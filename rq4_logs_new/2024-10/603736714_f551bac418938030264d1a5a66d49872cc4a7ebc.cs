using MessageBroker;
using Microsoft.Extensions.Configuration;

namespace HandlersTest.Builders.Events.NewCreatedUserEvent;
internal class NewUserCreatedTest : BaseMessageBrokerClient
{
    public NewUserCreatedTest(IConfiguration configuration) : base(configuration)
    {
    }

    public override void PreparePublish(DomainBaseEvents _domainEvent)
    {
        return;
    }

    protected override byte[] BuildMessage()
    {
        return new byte[0];
    }

    protected override void LoadConfig()
    {
        return;
    }
}