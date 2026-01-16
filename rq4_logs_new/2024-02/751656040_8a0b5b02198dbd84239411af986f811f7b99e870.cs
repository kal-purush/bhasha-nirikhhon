using System.Net;
using Microsoft.VisualBasic;

namespace Sprig.Core.Messages;

public static class Serializer
{
    public const int MessageSize = sizeof(int);
    public const int ResponseMessageSize = MessageSize + sizeof(int);
    public const int HandshakeRequestSize = MessageSize + sizeof(uint);
    public const int HandshakeResponseSize = ResponseMessageSize + sizeof(uint);

    public static byte[] Serialize(Message message)
    {
        byte[] serialized = message.Kind switch
        {
            MessageKind.HandshakeRequest => Serialize((HandshakeRequest)message),
            MessageKind.HandshakeResponse => Serialize((HandshakeResponse)message),
            _ => [],
        };
        return serialized;
    }

    public static byte[] Serialize(HandshakeRequest message)
    {
        var bytes = new byte[HandshakeRequestSize];
        var offset = SerializeBaseMessage(message, bytes);
        var desiredProtocolVersion = IPAddress.HostToNetworkOrder(message.DesiredProtocolVersion);
        BitConverter.TryWriteBytes(bytes.AsSpan()[offset..], desiredProtocolVersion);
        return bytes;
    }

    public static byte[] Serialize(HandshakeResponse message)
    {
        var bytes = new byte[HandshakeResponseSize];
        var offset = SerializeResponseMessage(message, bytes);
        var protocolVersion = IPAddress.HostToNetworkOrder(message.ProtocolVersion);
        BitConverter.TryWriteBytes(bytes.AsSpan()[offset..], protocolVersion);
        return bytes;
    }

    private static int SerializeResponseMessage(Response message, Span<byte> buffer)
    {
        var offset = SerializeBaseMessage(message, buffer);
        var responseCode = IPAddress.HostToNetworkOrder((int)message.Code);
        BitConverter.TryWriteBytes(buffer[offset..], responseCode);
        return ResponseMessageSize;
    }

    /// <summary>
    /// Serializes the base properties of a message
    /// </summary>
    /// <param name="message">The message to serialize.</param>
    /// <param name="buffer">The buffer to write into.</param>
    /// <returns>The number of bytes that were written.</returns>
    private static int SerializeBaseMessage(Message message, Span<byte> buffer)
    {
        int toWrite = IPAddress.HostToNetworkOrder((int)message.Kind);
        BitConverter.TryWriteBytes(buffer, toWrite);
        return MessageSize;
    }
}