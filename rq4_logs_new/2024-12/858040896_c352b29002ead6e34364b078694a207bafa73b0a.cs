using System.Net.Sockets;

namespace NetworkChat.Tests;

/// <summary>
/// Unit tests for chat utility methods.
/// </summary>
[TestFixture]
public class ChatUtilsTests
{
    /// <summary>
    /// Verifies that SendMessages writes data to a NetworkStream correctly.
    /// </summary>
    [Test]
    public async Task SendMessages_ShouldWriteToNetworkStream()
    {
        var listener = new TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;

        var clientTask = Task.Run(async () =>
        {
            using var client = new TcpClient("127.0.0.1", port);
            using var stream = client.GetStream();
            using var reader = new StreamReader(stream);
            var receivedMessage = await reader.ReadLineAsync();
            Assert.Equals("Test Message", receivedMessage);
        });

        using var server = await listener.AcceptTcpClientAsync();
        using var serverStream = server.GetStream();

        Console.SetIn(new StringReader("Test Message\nexit"));

        ChatUtils.SendMessages(serverStream);

        listener.Stop();
        await clientTask;
    }

    /// <summary>
    /// Verifies that ReceiveMessages reads data from a NetworkStream correctly.
    /// </summary>
    [Test]
    public async Task ReceiveMessages_ShouldReadFromNetworkStream()
    {
        var listener = new TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;

        var clientTask = Task.Run(async () =>
        {
            using var client = new TcpClient("127.0.0.1", port);
            using var stream = client.GetStream();
            using var writer = new StreamWriter(stream) { AutoFlush = true };
            await writer.WriteLineAsync("Test Message");
        });

        using var server = await listener.AcceptTcpClientAsync();
        using var serverStream = server.GetStream();

        var consoleOutput = new StringWriter();
        Console.SetOut(consoleOutput);

        var receiveTask = Task.Run(() => ChatUtils.ReceiveMessages(serverStream));
        await Task.Delay(500);

        Assert.AreEquals("Test Message", consoleOutput.ToString().Trim());

        listener.Stop();
        await clientTask;
    }
}