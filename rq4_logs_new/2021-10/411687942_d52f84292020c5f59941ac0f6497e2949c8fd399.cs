using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Consumer.Service.Handlers.Interfaces;
using Domain.Events;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Consumer.Service.Consumer
{
    public class MqConsumer
    {
        private IConnection _connection;
        private IModel _channel;
        private readonly IBookingsHandler _bookingsHandler; 
        
        public MqConsumer(IBookingsHandler bookingsHandler)
        {
            _bookingsHandler = bookingsHandler;
           setupConnection();
        }

        private void setupConnection()
        {
            var factory = new ConnectionFactory {HostName = "localhost"};

            _connection = factory.CreateConnection();
            // _connection.ConnectionShutdown += RabbitMQ_ShutdownConnection;
            _channel = _connection.CreateModel();
            _channel.QueueDeclare(
                queue: "classBooking", 
                durable: false, 
                exclusive: false, 
                autoDelete: false,
                arguments: null);
            var consumer = new EventingBasicConsumer(_channel);
            consumer.Received += (ch, ea) =>
            {
                var content = Encoding.UTF8.GetString(ea.Body.ToArray());
                var received = JsonConvert.DeserializeObject<BookClassEvent>(content);

                HandleMessage(received);
                _channel.BasicAck(ea.DeliveryTag, false);
            };
        }

        // protected override Task ExecuteAsync(CancellationToken stoppingToken)
        // {
        //     stoppingToken.ThrowIfCancellationRequested();
        //
        //     var consumer = new EventingBasicConsumer(_channel);
        //     consumer.Received += (ch, ea) =>
        //     {
        //         var content = Encoding.UTF8.GetString(ea.Body.ToArray());
        //         var received = JsonConvert.DeserializeObject<BookClassEvent>(content);
        //
        //         HandleMessage(received);
        //         _channel.BasicAck(ea.DeliveryTag, false);
        //     };
        //
        //     // consumer.Shutdown += ConsumerShutdown;
        //     // consumer.Registered += ConsumerRegistered;
        //     // consumer.Unregistered += ConsumerUnregistered;
        //     // consumer.ConsumerCancelled += ConsumerCancelled;
        //     
        //     _channel.BasicConsume("classBooking", false, consumer);
        //     
        //     return Task.CompletedTask;
        // }

        // private void ConsumerCancelled(object? sender, ConsumerEventArgs e)
        // {
        // }
        //
        // private void ConsumerUnregistered(object? sender, ConsumerEventArgs e)
        // {
        // }
        //
        // private void ConsumerRegistered(object? sender, ConsumerEventArgs e)
        // {
        // }
        //
        // private void ConsumerShutdown(object? sender, ShutdownEventArgs e)
        // {
        //     
        // }

        private void HandleMessage(BookClassEvent evt)
        {
            _bookingsHandler.HandleClassBooking(evt);
        }
        
        private void RabbitMQ_ShutdownConnection(object sender, ShutdownEventArgs e)
        {
            
        }
    }
}