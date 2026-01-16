namespace RedPandaBot.CLI.Commands;
public class HelloModule : ApplicationCommandModule
{
    [SlashCommand("hello", "Red Panda Bot greets you!")]
    public async Task HelloCommand(InteractionContext ctx)
    {
        await ctx.CreateResponseAsync(InteractionResponseType.ChannelMessageWithSource, new DiscordInteractionResponseBuilder().WithContent("Hello there!"));
    }

    public enum Choices 
    {
        [ChoiceName("Red Panda")]
        redPanda,
        [ChoiceName("Green Panda")]
        greenPanda,
    }

    [SlashCommand("choose", "Choose one of the following options:")]
    public async Task ChooseCommand(InteractionContext ctx, [Option("color", "Panda color")] Choices choice = Choices.redPanda)
    {
        var response = choice switch
        {
            Choices.redPanda => "Wise choice! A Red Panda should be red!",
            Choices.greenPanda => "Green Panda? Ewww",
        };
        await ctx.CreateResponseAsync(InteractionResponseType.ChannelMessageWithSource, new DiscordInteractionResponseBuilder().WithContent(response));
    }
}