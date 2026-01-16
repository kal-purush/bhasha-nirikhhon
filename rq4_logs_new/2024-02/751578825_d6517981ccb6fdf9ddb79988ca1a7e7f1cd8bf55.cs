using Coravel.Events.Interfaces;
using Coravel.Invocable;
using PalworldDaemon.Events;

namespace PalworldDaemon.Invocables;

public class ShutdownPalworldServerJob(
    ILogger<ShutdownPalworldServerJob> logger,
    IDispatcher dispatcher,
    IPalworldRCONConnection rconConnection) 
    : IInvocable, ICancellableInvocable
{
    public async Task Invoke()
    {
        await dispatcher.Broadcast(new ShutdownWarning { ShutdownTime = TimeSpan.FromMinutes(5) });

        logger.LogDebug("Waiting for 4 minutes before sending shutdown message");
        await Task.Delay(TimeSpan.FromMinutes(4), CancellationToken).ConfigureAwait(false);

        logger.LogDebug("Sending shutdown command and 1 minute warning message to RCON");
        await rconConnection.ShutdownAsync(TimeSpan.FromMinutes(1), "Server Restart In 1min LOGOUT NOW!!!",
                CancellationToken)
            .ConfigureAwait(false);
    }

    public CancellationToken CancellationToken { get; set; }
}