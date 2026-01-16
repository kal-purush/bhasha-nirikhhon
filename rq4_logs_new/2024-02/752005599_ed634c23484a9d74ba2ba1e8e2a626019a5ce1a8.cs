using Microsoft.Extensions.Logging;
using System;

namespace UWorx.JiraWorkLogs.RabbitMQ;

public class RabbitMQReceiverService : IServiceMessagingService
{
    private readonly MessageReceiver messageReceiver;

    public event EventHandler<ActivityEventArgs> OnMessageReceived;

    public RabbitMQReceiverService(ILogger<RabbitMQReceiverService> logger, string host, string user, string password)
    {
        this.messageReceiver = new MessageReceiver(logger, host, user, password);
    }

    public void StartConsumer()
    {
        this.messageReceiver.OnMessageReceived += (s, e) => this.OnMessageReceived?.Invoke(this, e);
        this.messageReceiver.StartConsumer();
    }
}