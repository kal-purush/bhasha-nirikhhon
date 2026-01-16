using Replybot.Models;

namespace Replybot.TextCommands.Definitions;

public class AsyncCommandHandlerDefinition : ICommandHandlerDefinition
{
    /// <summary>
    ///     The keyword to match for triggering this command
    /// </summary>
    public TriggerKeyword TriggerKeyword { get; set; }
    /// <summary>
    ///     The function to call if this command runs
    /// </summary>
    public Func<SocketMessage, Task<Embed?>> HandleCommand { get; set; }
    /// <summary>
    ///     If this command runs, stop checking for commands afterwards
    /// </summary>
    public bool StopCheckingForCommands { get; set; }
}