using Replybot.BusinessLayer;
using Replybot.TextCommands.Models;

namespace Replybot.TextCommands;

public class FlipACoinCommand : ITextCommand
{
    private readonly IReplyBusinessLayer _replyBusinessLayer;
    private readonly string[] _triggers = { "flip a coin", "🪙", "heads or tails" };
    private readonly string[] _replies = { "Heads 🪙", "Tails 🪙" };

    public FlipACoinCommand(IReplyBusinessLayer replyBusinessLayer)
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
        return Task.FromResult(new CommandResponse
        {
            Description = _replyBusinessLayer.ChooseReply(_replies),
            Reactions = null,
            StopProcessing = true
        });
    }
}