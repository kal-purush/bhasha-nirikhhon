using Discord.WebSocket;

namespace DemocracyBot.Commands.ExecutionFunctions; 

public class DevToolsCommand : ICommandExecutionHandler {
    public async Task Execute(SocketSlashCommand cmd, DiscordSocketClient client) {
        string? devCmd = cmd.GetArgument<string?>("command");

        if (devCmd == null) {
            await Respond(cmd, "You need to specify a command to run.");
            return;
        }

        switch (devCmd) {
            default:
                await Respond(cmd, "Yes but also no");
                return;
            
            case "throw":
                throw new Exception("This is a test exception");
        }
    }

    public static async Task Respond(SocketSlashCommand cmd, string msg) {
        await cmd.RespondWithEmbedAsync("Dev Tools", msg);
    }
}