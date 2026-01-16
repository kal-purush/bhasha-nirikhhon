using Messaging.Buffer.Attributes;
using Messaging.Buffer.Buffer;
using Messaging.Buffer.TestApp.Requests;
using Microsoft.Extensions.Logging;

namespace Messaging.Buffer.TestApp.Handlers
{
    [Handler]
    public class HelloWorldHandler : HandlerBase<HelloWorldRequest>
    {
        public HelloWorldHandler(ILogger<HelloWorldHandler> logger, IMessaging messaging) : base(logger, messaging)
        {
        }

        public override async void Handle<TRequest>(string correlationId, TRequest request)
        {
            _logger.LogTrace($"Handle: {nameof(HelloWorldHandler)}");

            await base.Respond(correlationId, new HelloWorldResponse(correlationId)
            {
                InstanceResponse = $"Hello from {Environment.UserName} - from a handler."
            });
        }
    }
}