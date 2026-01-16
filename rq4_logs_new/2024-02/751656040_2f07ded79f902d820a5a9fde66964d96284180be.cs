using System.Net.Sockets;
using Sprig.Core.Messages;

namespace Sprig.Core;

public static class MessageStream
{
    public static async Task<Message> ReadAsync(NetworkStream stream, CancellationToken cancellationToken)
    {
        var messageBuffer = new byte[Serializer.MessageMaxSize];
        Message baseMessage = await ReadBaseMessageAsync(stream, messageBuffer, cancellationToken);
        return await ReadRestOfMessageAsync(stream, messageBuffer, baseMessage, cancellationToken);
    }

    public static async Task<Message?> ReadAsync(NetworkStream stream, MessageKind expected, CancellationToken cancellationToken)
    {
        var messageBuffer = new byte[Serializer.MessageMaxSize];
        Message baseMessage = await ReadBaseMessageAsync(stream, messageBuffer, cancellationToken);
        if (baseMessage.Kind != expected)
        {
            return null;
        }

        return await ReadRestOfMessageAsync(stream, messageBuffer, baseMessage, cancellationToken);
    }

    private static async Task<Message> ReadBaseMessageAsync(NetworkStream stream,
        byte[] messageBuffer,
        CancellationToken cancellationToken)
    {
        var bytesRead = await stream.ReadAsync(messageBuffer, 0, Serializer.MessageSize, cancellationToken);
        if (bytesRead < Serializer.MessageSize)
        {
            // I don't know what to do here.
            throw new InvalidOperationException();
        }

        var baseMessage = Serializer.DeserializeBaseMessage(messageBuffer.AsSpan()[0..Serializer.MessageSize]);
        return baseMessage;
    }

    private static async Task<Message> ReadRestOfMessageAsync(NetworkStream stream,
        byte[] messageBuffer,
        Message baseMessage,
        CancellationToken cancellationToken)
    {
        if (Serializer.MessageKindSize.TryGetValue(baseMessage.Kind, out var messageSize))
        {
            var restOfMessageSize = messageSize - Serializer.MessageSize;
            var bytesRead = await stream.ReadAsync(messageBuffer.AsMemory()[Serializer.MessageSize..messageSize], cancellationToken);
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