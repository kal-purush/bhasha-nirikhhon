using DiscordDotNetUtilities.Interfaces;
using Replybot.BusinessLayer;
using Replybot.TextCommands.Models;

namespace Replybot.TextCommands;

public class GetChannelAgeCommand : ITextCommand
{
    private readonly IReplyBusinessLayer _replyBusinessLayer;
    private readonly IDiscordFormatter _discordFormatter;
    private readonly string[] _triggers = { "channel age", "how old is this channel", "when did this channel start", "when was this channel made" };

    public GetChannelAgeCommand(IReplyBusinessLayer replyBusinessLayer,
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
        var channelAgeText = GetChannelAgeText(message.Channel as SocketGuildChannel);
        var embed = _discordFormatter.BuildRegularEmbed("Server Member Count",
            $"This channel{channelAgeText}", message.Author);

        return Task.FromResult(new CommandResponse
        {
            Embed = embed,
            Reactions = null,
            StopProcessing = true
        });
    }

    private static string GetChannelAgeText(SocketGuildChannel? socketGuildChannel)
    {
        if (socketGuildChannel == null)
        {
            return "...is not a channel.";
        }

        var createdAtDate = socketGuildChannel.CreatedAt;
        var timeAgo = DateTime.UtcNow - createdAtDate;
        return $" was created on **{createdAtDate}** ({timeAgo:d'd 'h'h 'm'm 's's'} ago)";
    }
}