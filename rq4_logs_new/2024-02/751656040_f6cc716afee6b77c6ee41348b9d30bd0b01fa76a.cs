using System.Net;
using System.Net.Sockets;
using Sprig.Core.Messages;

namespace Sprig.Server;

class Server
{
    private readonly TcpListener _listener;

    public Server()
    {
        _listener = new TcpListener(IPAddress.Any, 8080);
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        _listener.Start();
        var buffer = new byte[Serializer.MessageMaxSize];

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var clientSocket = await _listener.AcceptSocketAsync(cancellationToken);
                if (await HandleHandshake(buffer, clientSocket, cancellationToken))
                {
                    await ProcessClient(clientSocket, cancellationToken);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _listener.Stop();
        }
    }

    private Task ProcessClient(Socket client, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            if (client.Poll(TimeSpan.FromMilliseconds(10), SelectMode.SelectRead))
            {
                // Maybe a message?
            }
        }

        return Task.CompletedTask;
    }

    private static async Task<bool> HandleHandshake(byte[] buffer, Socket clientSocket, CancellationToken cancellationToken)
    {
        var bytesReceived = await clientSocket.ReceiveAsync(buffer, SocketFlags.None, cancellationToken);
        if (bytesReceived > 0)
        {
            var message = Serializer.Deserialize(buffer.AsSpan()[..bytesReceived]);
            if (message.Kind != MessageKind.HandshakeRequest)
            {
                clientSocket.Close();
                return false;
            }

            var handshakeRequest = (HandshakeRequest)message;
            HandshakeResponse response;
            if (handshakeRequest.DesiredProtocolVersion == 1)
            {
                response = HandshakeResponse.Accept(1);
            }
            else
            {
                response = HandshakeResponse.Reject(1);
            }

            var responseBytes = Serializer.Serialize(response);
            await clientSocket.SendAsync(responseBytes, cancellationToken);
            return true;
        }
        else
        {
            clientSocket.Close();
            return false;
        }
    }
}