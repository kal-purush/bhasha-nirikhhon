using Replybot.BusinessLayer;
using Replybot.Models;
using Replybot.TextCommands.Models;

namespace Replybot.TextCommands;

public class VersionCommand : ITextCommand
{
    private readonly IReplyBusinessLayer _replyBusinessLayer;
    private readonly VersionSettings _versionSettings;
    private readonly string[] _triggers = { "-v", "version", "what version" };
    private const string VersionNumberKeyword = "{{VERSIONNUMBER}}";
    private readonly string[] _replies = {
        $"I am {VersionNumberKeyword} versions old.",
        $"That's kind of private, but if you must know, I'm at v{VersionNumberKeyword}.",
        $"I'm at v{VersionNumberKeyword}, what version are you at?"
    };

    public VersionCommand(IReplyBusinessLayer replyBusinessLayer, VersionSettings versionSettings)
    {
        _replyBusinessLayer = replyBusinessLayer;
        _versionSettings = versionSettings;
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
            Description = _replyBusinessLayer.ChooseReply(_replies)?.Replace(VersionNumberKeyword, _versionSettings.VersionNumber),
            Reactions = null,
            StopProcessing = true
        });
    }
}