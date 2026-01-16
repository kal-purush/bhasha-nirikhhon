using System.Net;
using System.Net.Sockets;
using Sprig.Core.Messages;

namespace Sprig.Client;

class Client
{
    private readonly TcpClient _client;
    public Client(string url, int port)
    {
        _client = new TcpClient(url, port);
    }

    public async Task SendAsync(Message message, CancellationToken cancellationToken)
    {
        var buffer = Serializer.Serialize(message);
        var networkStream = _client.GetStream();
        await networkStream.WriteAsync(buffer, cancellationToken);
    }

    public async Task<Message> ReceiveAsync(CancellationToken cancellationToken)
    {
        var networkStream = _client.GetStream();
        var messageBuffer = new byte[Serializer.MessageMaxSize];

        var bytesRead = await networkStream.ReadAsync(messageBuffer, 0, Serializer.MessageSize, cancellationToken);
        if (bytesRead < Serializer.MessageSize)
        {
            // I don't know what to do here.
            throw new InvalidOperationException();
        }

        var baseMessage = Serializer.DeserializeBaseMessage(messageBuffer.AsSpan()[0..Serializer.MessageSize]);
        if (Serializer.MessageKindSize.TryGetValue(baseMessage.Kind, out var messageSize))
        {
            var restOfMessageSize = messageSize - Serializer.MessageSize;
            bytesRead = await networkStream.ReadAsync(messageBuffer.AsMemory()[Serializer.MessageSize..messageSize], cancellationToken);
            if (bytesRead != restOfMessageSize)
            {
                // I don't know what to do here.
                throw new InvalidOperationException();
            }

            return Serializer.Deserialize(messageBuffer);
        }
        else
        {
            // I don't know what to do here.
            throw new InvalidOperationException();
        }
    }
}