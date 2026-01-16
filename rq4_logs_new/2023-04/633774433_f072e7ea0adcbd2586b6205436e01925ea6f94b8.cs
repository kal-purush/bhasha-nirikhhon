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

var exchangeName = "logs";

// define an exchange (broadcast)
channel.ExchangeDeclare(exchange: exchangeName, type: ExchangeType.Fanout);

var message = GetMessage(args);
var body = Encoding.UTF8.GetBytes(message);
channel.BasicPublish(
    exchange: exchangeName,
    routingKey: string.Empty,
    basicProperties: null,
    body: body
);

Console.WriteLine($" [x] Sent {message}");
Console.WriteLine($" Press [Enter] to exit");
Console.ReadKey();

static string GetMessage(string[] args)
{
    return ((args.Length > 0) ? string.Join(" ", args) : "info: Hello World!");
}