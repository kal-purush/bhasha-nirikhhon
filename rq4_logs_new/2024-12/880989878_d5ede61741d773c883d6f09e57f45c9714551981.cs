using MassTransit;
using Microsoft.Extensions.Logging;

namespace MarGate.Core.MessageBus;

public abstract class MessageBaseConsumer<TMessage> : IMessageConsumer<TMessage> where TMessage : class, IMessage
{
    private readonly ILogger _logger;

    protected MessageBaseConsumer(ILogger logger)
    {
        this._logger = logger;
    }

    public Task Consume(ConsumeContext<TMessage> context)
    {
        try
        {
            return ConsumeAsync(context);
        }
        catch (Exception)
        {
            _logger.LogError($"Message : {nameof(context.Message)} is failed.");
            throw;
        }
    }

    public abstract Task ConsumeAsync(ConsumeContext<TMessage> context);
}
}