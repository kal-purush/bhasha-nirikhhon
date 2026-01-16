using DiscordDotNetUtilities.Interfaces;
using Replybot.BusinessLayer;
using Replybot.TextCommands.Models;

namespace Replybot.TextCommands;

public class GetMemberCountCommand : ITextCommand
{
    private readonly IReplyBusinessLayer _replyBusinessLayer;
    private readonly IDiscordFormatter _discordFormatter;
    private readonly string[] _triggers = { "how many members", "how many people", "member count", "people count", "headcount", "head count" };

    public GetMemberCountCommand(IReplyBusinessLayer replyBusinessLayer,
        IDiscordFormatter discordFormatter)
    {
        _replyBusinessLayer = replyBusinessLayer;
        _discordFormatter = discordFormatter;
    }

    public bool CanHandle(TextCommandReplyCriteria replyCriteria)
    {
        return replyCriteria.IsBotNameMentioned &&
               _triggers.Any(t => _replyBusinessLayer.GetWordMatch(t, replyCriteria.MessageText));
    }

    public Task<CommandResponse> Handle(SocketMessage message)
    {
        if (message.Channel is not IGuildChannel { Guild: SocketGuild guild })
        {
            return Task.FromResult(new CommandResponse
            {
                Embed = _discordFormatter.BuildErrorEmbed("Not a Server",
                    "This command can only be used in a Discord server, it will not work in a DM.", message.Author),
                StopProcessing = true
            });
        }

        var embed = _discordFormatter.BuildRegularEmbed("Server Member Count",
            $"This server has **{guild.MemberCount}** members.", message.Author);

        return Task.FromResult(new CommandResponse
        {
            Embed = embed,
            Reactions = null,
            StopProcessing = true
        });
    }
}