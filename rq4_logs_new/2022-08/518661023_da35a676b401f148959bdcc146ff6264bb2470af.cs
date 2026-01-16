using DemocracyBot.Data.Schemas;
using Discord.WebSocket;

namespace DemocracyBot.Commands.ExecutionFunctions; 

public class RiotCommand : ICommandExecutionHandler {
    public async void Execute(SocketSlashCommand cmd, DiscordSocketClient client) {
        Term? term = Program.StorageService.GetCurrentTerm();
        if (term == null) {
            await cmd.RespondWithEmbedAsync("Error", "No term is currently in progress.", ResponseType.Error);
            return;
        }

        if (term.RiotVotes.Contains(cmd.User.Id)) {
            term.RiotVotes.Remove(cmd.User.Id);
            Program.StorageService.SetCurrentTerm(term);
            await cmd.RespondWithEmbedAsync("Go Home", "You have removed your request to overthrow the president", ResponseType.Success);
            return;
        }
        term.RiotVotes.Add(cmd.User.Id);
        Program.StorageService.SetCurrentTerm(term);
        await cmd.RespondWithEmbedAsync("Off With His Head", "You have voted to overthrow the president", ResponseType.Success);
    }
}