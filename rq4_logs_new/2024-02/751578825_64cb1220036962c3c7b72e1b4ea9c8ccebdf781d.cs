using System.Net;
using CoreRCON;

namespace PalworldDaemon;

internal class PalworldRconConnection(int port, string password, ILogger<PalworldRconConnection> logger) : IPalworldRCONConnection
{
    private RCON _rconConnection = new(IPEndPoint.Parse($"127.0.0.1:{port}"), password, logger: logger);
    
    public async Task BroadcastAsync(string message, CancellationToken cancellationToken = default)
    {
        logger.LogDebug("Broadcast connecting");
        await _rconConnection.ConnectAsync().ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();

        logger.LogDebug("Broadcast: {message}", message);
        await SendCommandAsync($"broadcast {message.Replace(" ", "")}").ConfigureAwait(false);
    }

    public async Task ShutdownAsync(TimeSpan shutdownInTimeSpan, string message, CancellationToken cancellationToken = default)
    {
        logger.LogDebug("Shutdown connecting");
        await _rconConnection.ConnectAsync().ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        
        logger.LogDebug("Shutdown in {shutdownInTimeSpan}: {message}", shutdownInTimeSpan, message);
        await SendCommandAsync($"shutdown {shutdownInTimeSpan.TotalSeconds} {message.Replace(" ", "")}").ConfigureAwait(false); // Palworld doesn't support spaces in broadcasts?
    }

    private async Task SendCommandAsync(string commandText)
    {
        try
        {
            await _rconConnection.SendCommandAsync(commandText)
                .ConfigureAwait(false); // Palworld doesn't support spaces in broadcasts?
        }
        catch (TimeoutException)
        {
            // Ignored because I don't think Palworld sends a response
        }
    }

    public void Dispose()
    {
        _rconConnection.Dispose();
    }
}