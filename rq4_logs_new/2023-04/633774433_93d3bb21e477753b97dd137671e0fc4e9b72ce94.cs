using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

var factory = new ConnectionFactory
{
    HostName = "192.168.26.211",
    Port = 5672,
    UserName = "sadmin",
    Password = "sadmin",
    VirtualHost = "/"
};

using var connection = factory.CreateConnection();
using var channel = connection.CreateModel();

channel.QueueDeclare(
    queue: "hello",
    durable: false,
    exclusive: false,
    autoDelete: false,
    arguments: null
);

Console.WriteLine(" [*] Waiting for messages.");

var consumer = new EventingBasicConsumer(channel);
consumer.Received += (model, evtArgs) => 
{
    var body = evtArgs.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);
    Console.WriteLine($" [x] Received message {message}");
};

channel.BasicConsume(
    queue: "hello",
    autoAck: true,
    consumer: consumer
);

Console.WriteLine("Press Enter to exit.");
Console.ReadKey(false);