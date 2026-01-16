using Replybot.BusinessLayer;
using Replybot.TextCommands.Models;

namespace Replybot.TextCommands;

public class SearchCommand : ITextCommand
{
    private readonly IReplyBusinessLayer _replyBusinessLayer;
    private readonly List<SearchLinkMap> _searchLinkMappings = new()
    {
        new SearchLinkMap(new List<string>{"search", "look up", "ddg", "duckduckgo"}, "DuckDuckGo", "https://duckduckgo.com/?q="),
        new SearchLinkMap(new List<string>{"google"}, "Google", "https://www.google.com/search?q="),
        new SearchLinkMap(new List<string>{"bing", "bing it"}, "Bing", "https://www.bing.com/search?q="),
    };

    private readonly List<string> _triggers = new();

    public SearchCommand(IReplyBusinessLayer replyBusinessLayer)
    {
        _replyBusinessLayer = replyBusinessLayer;
        foreach (var searchLinkMapping in _searchLinkMappings)
        {
            _triggers.AddRange(searchLinkMapping.Triggers);
        }
    }

    public bool CanHandle(TextCommandReplyCriteria replyCriteria)
    {
        return replyCriteria.IsBotNameMentioned &&
               _triggers.Any(t => _replyBusinessLayer.GetWordMatch(t, replyCriteria.MessageText));
    }

    public Task<CommandResponse> Handle(SocketMessage message)
    {
        var searchToUse = GetSearchToUse(message.Content);

        var messageEncodedWithoutTriggers = message.Content.RemoveTriggersFromMessage(searchToUse.Triggers.ToArray());

        return Task.FromResult(new CommandResponse
        {
            Description = $"{searchToUse.Url}{messageEncodedWithoutTriggers}",
            Reactions = null,
            StopProcessing = true
        });
    }

    private SearchLinkMap GetSearchToUse(string messageContent)
    {
        var searchToUse = _searchLinkMappings.FirstOrDefault(searchLinkMap => searchLinkMap.Triggers.Any(trigger =>
            _replyBusinessLayer.GetWordMatch(trigger, messageContent)));

        return searchToUse ?? _searchLinkMappings.First();
    }
}