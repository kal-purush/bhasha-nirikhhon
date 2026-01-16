using Sancho.Client.Core;
using Serilog;

namespace ConsoleClient
{
    class EchoPlugin : IPlugin
    {
        private Connection _connection;

        public EchoPlugin(Connection connection)
        {
            _connection = connection;
        }

        public string Name => "echo";

        public void Recieve(Message message)
        {
            Log.Information($"Received message {message.command} {message.data} from {message.metadata.senderId}");

            _connection.SendAsync(Name, message.command, message.data, message.metadata.messageId);
        }
    }
}