using DiscordDotNetUtilities.Interfaces;
using Replybot.BusinessLayer;
using Replybot.TextCommands.Models;

namespace Replybot.TextCommands;

public class CanIStreamCommand : ITextCommand
{
    private readonly IReplyBusinessLayer _replyBusinessLayer;
    private readonly IDiscordFormatter _discordFormatter;
    private const string JustWatchBaseUrl = "https://www.justwatch.com/";
    private const string Description = "Use one of the following links to see streaming availability search results in the specified country. For other countries, click one of these links and change your country on the website!";
    private readonly string[] _triggers = { "stream", "can i watch", "can i stream", "justwatch", "just watch" };
    private readonly List<StreamLinkUrlMap> _streamLinkMappings = new()
    {
        new StreamLinkUrlMap(":flag_us:","USA","us","search"),
        new StreamLinkUrlMap(":flag_ca:","Canada","ca","search"),
        new StreamLinkUrlMap(":flag_gb:","UK","uk","search"),
        new StreamLinkUrlMap(":flag_de:","Germany","de","Suche"),
        new StreamLinkUrlMap(":flag_br:","Brazil","br","busca"),
        new StreamLinkUrlMap(":flag_in:","India","in","search"),
        new StreamLinkUrlMap(":flag_om:","Oman","om","search"),
    };

    public CanIStreamCommand(IReplyBusinessLayer replyBusinessLayer,
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
        var embed = GetStreamLinksEmbed(message);

        return Task.FromResult(new CommandResponse
        {
            Embed = embed,
            Reactions = null,
            StopProcessing = true
        });
    }

    private Embed GetStreamLinksEmbed(SocketMessage message)
    {
        var messageEncodedWithoutTriggers = message.Content.RemoveTriggersFromMessage(_triggers);

        var embedFieldBuilders = _streamLinkMappings.Select(streamLinkUrlMap => new EmbedFieldBuilder
        {
            Name = $"{streamLinkUrlMap.Flag} {streamLinkUrlMap.Name}",
            Value = $"{JustWatchBaseUrl}{streamLinkUrlMap.CountryCode}/{streamLinkUrlMap.SearchWord}?q={messageEncodedWithoutTriggers}",
            IsInline = false
        }).ToList();

        return _discordFormatter.BuildRegularEmbed("Stream Link Options", Description, message.Author,
            embedFieldBuilders);
    }
}