using System.Diagnostics;
using System.Text;
using RabbitMQ.Client.Events;
using RabbitMQ.Client;

namespace CuratorMagazineBlazorApp.RabbitMq;

/// <summary>
/// Class RabbitMqListener.
/// Implements the <see cref="Microsoft.Extensions.Hosting.BackgroundService" />
/// </summary>
/// <seealso cref="Microsoft.Extensions.Hosting.BackgroundService" />
public class RabbitMqListener : BackgroundService
{
    /// <summary>
    /// The connection
    /// </summary>
    private IConnection _connection;
    /// <summary>
    /// The channel
    /// </summary>
    private IModel _channel;

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitMqListener"/> class.
    /// </summary>
    public RabbitMqListener()
    {
        var factory = new ConnectionFactory { HostName = "localhost" };
        _connection = factory.CreateConnection();
        _channel = _connection.CreateModel();
        _channel.QueueDeclare(queue: "MyQueue", durable: false, exclusive: false, autoDelete: false, arguments: null);
    }

    /// <summary>
    /// This method is called when the <see cref="T:Microsoft.Extensions.Hosting.IHostedService" /> starts. The implementation should return a task that represents
    /// the lifetime of the long running operation(s) being performed.
    /// </summary>
    /// <param name="stoppingToken">Triggered when <see cref="M:Microsoft.Extensions.Hosting.IHostedService.StopAsync(System.Threading.CancellationToken)" /> is called.</param>
    /// <returns>A <see cref="T:System.Threading.Tasks.Task" /> that represents the long running operations.</returns>
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        stoppingToken.ThrowIfCancellationRequested();

        var consumer = new EventingBasicConsumer(_channel);
        consumer.Received += (ch, ea) =>
        {
            var content = Encoding.UTF8.GetString(ea.Body.ToArray());
            
            Debug.WriteLine($"Получено сообщение: {content}");

            _channel.BasicAck(ea.DeliveryTag, false);
        };

        _channel.BasicConsume("MyQueue", false, consumer);

        return Task.CompletedTask;
    }

    /// <summary>
    /// Disposes this instance.
    /// </summary>
    public override void Dispose()
    {
        _channel.Close();
        _connection.Close();
        base.Dispose();
    }
}