using System.Text;
using RabbitMQ.Client;

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

const string queueName = "task_queue";
channel.QueueDeclare(
    queue: queueName,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: null
);

string message = GetMessage(args);
static string GetMessage(string[] args)
{
    return args.Length > 0 ? string.Join(" ", args): "Hello World!";
}


var body = Encoding.UTF8.GetBytes(message);

// Persistent Message in Broker
var props = channel.CreateBasicProperties();
props.Persistent = true;

channel.BasicPublish(
    exchange: string.Empty,
    routingKey: queueName,
    basicProperties: null,
    body: body
);

Console.WriteLine($"[x] Sent {message}");
// Console.WriteLine("Press Enter to exit.");
// Console.ReadKey(false);