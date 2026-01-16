using Replybot.BusinessLayer;
using Replybot.TextCommands.Models;

namespace Replybot.TextCommands;

public class SongLinkCommand : ITextCommand
{
    private readonly IReplyBusinessLayer _replyBusinessLayer;
    private const string SongLinkBaseUrl = "https://song.link/";
    private readonly string[] _triggers = { "songlink", "song link" };

    public SongLinkCommand(IReplyBusinessLayer replyBusinessLayer)
    {
        _replyBusinessLayer = replyBusinessLayer;
    }

    public bool CanHandle(TextCommandReplyCriteria replyCriteria)
    {
        return replyCriteria.IsBotNameMentioned &&
               _triggers.Any(t => _replyBusinessLayer.GetWordMatch(t, replyCriteria.MessageText));
    }

    public Task<CommandResponse> Handle(SocketMessage message)
    {
        var messageEncodedWithoutTriggers = message.Content.RemoveTriggersFromMessage(_triggers);

        return Task.FromResult(new CommandResponse
        {
            Description = $"{SongLinkBaseUrl}{messageEncodedWithoutTriggers}",
            Reactions = null,
            StopProcessing = true
        });
    }
}