using DemocracyBot.Data.Schemas;
using Discord.WebSocket;
using GeneralPurposeLib;

namespace DemocracyBot.Commands.ExecutionFunctions; 

public class ResignCommand : ICommandExecutionHandler {
    public async void Execute(SocketSlashCommand cmd, DiscordSocketClient client) {
        Term? term = Program.StorageService.GetCurrentTerm();
        if (term == null) {
            NotPresident(cmd);
            return;
        }
        if (term.PresidentId != cmd.User.Id) {
            NotPresident(cmd);
            return;
        }

        await GetAnnouncementsChannel(client)
            .SendMessageAsync(embed: 
                CommandManager.GetEmbed(
                    "Announcement", 
                    $"{cmd.User.Mention} has resigned as president.", 
                    ResponseType.Info).Build());
        
        term.TermEnd = DateTime.UtcNow.ToBinary();
        Program.StorageService.SetCurrentTerm(term);
        await cmd.RespondWithEmbedAsync("Adios", "You have resigned as president.", ResponseType.Success);
    }

    private static async void NotPresident(SocketSlashCommand cmd) {
        await cmd.RespondWithEmbedAsync(
            "Error", 
            "You are not the president and therefore cannot resign.",
            ResponseType.Error);
    }
    
    private static SocketTextChannel GetAnnouncementsChannel(DiscordSocketClient client) {
        if (client == null) throw new Exception("client is null");
        Logger.Debug(Program.Config!["server_id"]);
        SocketGuild? guild = client.GetGuild(ulong.Parse(Program.Config["server_id"]));
        if (guild == null) {
            Logger.Debug("guild is null");
        }
        SocketGuildChannel? server = guild!.GetChannel(ulong.Parse(Program.Config["announcements_channel_id"]));
        if (server == null) {
            Logger.Debug("server is null");
        }
        return (SocketTextChannel) server!;
    }
}