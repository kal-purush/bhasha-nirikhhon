using DSharpPlus;
using DSharpPlus.EventArgs;
using ShadowRoller.Domain.Contexts;
using ShadowRoller.Domain.Contexts.Dice;
using ShadowRoller.Domain.Contexts.ShadowRun;

namespace ShadowRoller.DiscordBot;

public class DiscordBot
{
    private readonly IRollContextParser<DiceRollContext, DiceModifierSumRollResult> _diceRollContextParser = new DiceRollContextParser();
    private readonly IRollContextParser<ShadowRunRollContext, ShadowRunRollResult> _shadowRunRollContext = new ShadowRunContextParser();
    private const string PREFIX = "!sr-";
    private const string DELIMITER = " ";
    private readonly CancellationTokenSource _cancellationTokenSource;

    public DiscordBot(DiscordClient discordClient, CancellationTokenSource cancellationTokenSource)
    {
        _cancellationTokenSource = cancellationTokenSource;
        discordClient.MessageCreated += OnMessageCreated;
    }

    private async Task OnMessageCreated(DiscordClient sender, MessageCreateEventArgs e)
    {
        if (!e.Message.Content.StartsWith(PREFIX, StringComparison.InvariantCultureIgnoreCase))
            return;

        var messageParts = e.Message.Content.Split(DELIMITER).ToArray();
        var command = messageParts.First().Replace(PREFIX, "").ToLower();
        var args = messageParts.Skip(1).ToArray();

        switch (command)
        {
            case "exit":
                await sender.SendMessageAsync(e.Message.Channel, "Ok, I'm leaving");
                _cancellationTokenSource.Cancel();
                break;
            case "sr5":
                var srContext = _shadowRunRollContext.ParseToRollContext(args);
                var srResult = srContext.Resolve();
                await PrintResult(sender, e, srResult);
                break;
            case "roll":
                var diceContext = _diceRollContextParser.ParseToRollContext(args);
                var diceResult = diceContext.Resolve();
                await PrintResult(sender, e, diceResult);
                break;
        }
    }

    private async Task PrintResult(DiscordClient sender, MessageCreateEventArgs e, IRollResult rollResult)
    {
        var result = rollResult.ToString(e.Message.Author.Username);
        await sender.SendMessageAsync(e.Message.Channel, result);
    }

}