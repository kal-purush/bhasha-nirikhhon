using Messaging.Buffer.Attributes;
using Messaging.Buffer.Buffer;
using Messaging.Buffer.TestApp.Requests;
using Microsoft.Extensions.Logging;

namespace Messaging.Buffer.TestApp.Handlers
{
    [Handler]
    public class TotalCountHandler : HandlerBase<TotalCountRequest>
    {
        public TotalCountHandler(ILogger<TotalCountHandler> logger, IMessaging messaging) : base(logger, messaging)
        {
        }

        public override async void Handle(string correlationId, TotalCountRequest request)
        {
            _logger.LogTrace($"Handle: {nameof(HelloWorldHandler)}");

            await base.Respond(correlationId, new TotalCountResponse(correlationId, 5));
        }
    }
}