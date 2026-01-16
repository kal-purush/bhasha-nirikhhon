using DSharpPlus.Entities;
using DSharpPlus.EventArgs;
using DSharpPlus.SlashCommands;
using GiphyDotNet.Model.Parameters;
using Microsoft.Extensions.Logging;
using OpenAI.GPT3.ObjectModels;
using OpenAI.GPT3.ObjectModels.RequestModels;
using OpenAI.GPT3.ObjectModels.ResponseModels;
using System.Text.RegularExpressions;
using static Robit.WordFilter.WordFilter;

namespace Robit.Response
{
    /// <summary>
    /// Handles AI interactions
    /// </summary>
    public static class AI
    {
        /// <summary>
        /// Generates a response intended for use in chat conversations. For text prompt generation use <c>GeneratePromptResponse()</c>.
        /// As that won't include any previous message context and execute faster because of that
        /// </summary>
        /// <param name="messageArgs">Discord message creation arguments</param>
        /// <returns>
        /// <list type="table">
        /// <listheader>A tuple containing response information</listheader>
        /// <item>
        /// <list type="table">
        /// <listheader>Item1 (bool)</listheader>
        /// <item>True: Generation successful</item>
        /// <item>False: Generation failed</item>
        /// </list>
        /// </item>
        /// <item>
        /// <list type="table">
        /// <listheader>Item2 (string)</listheader>
        /// <item>Generation successful: Generation result</item>
        /// <item>Generation failiure: Fail reason</item>
        /// </list>
        /// </item>
        /// </list>
        /// </returns>
        /// <exception cref="NullReferenceException">OpenAI text generation failed with an unknown error</exception>
        public static async Task<Tuple<bool, string>> GenerateChatResponse(MessageCreateEventArgs messageArgs)
        {
            Tuple<bool, string?> filter = Check(messageArgs.Message.Content);

            if (filter.Item1)
            {
                return Tuple.Create(false, "Message contained blacklisted word/topic");
            }

            if (AICheck(messageArgs.Message.Content).Result)
            {
                return Tuple.Create(false, "Message blocked by automod");
            }

            string displayName = messageArgs.Guild.CurrentMember.DisplayName;
            string discriminator = messageArgs.Guild.CurrentMember.Discriminator;
            string userID = messageArgs.Guild.CurrentMember.Id.ToString();

            List<ChatMessage> messages = new List<ChatMessage>()
            {
                ChatMessage.FromSystem
                    (
                          $"You are {displayName}. {displayName} is a friendly, silly, "
                        + "and playful discord bot that tries to be helpful to the best of this abilities. "
                        + "You show some personality and quirkiness. "
                        + "For example when asked \"How are you?\" you answer with something like \"Not much, just counting electric sheep\" (This is a rare answer, come up with something else). "
                        + "You adhere to the principles of Adeptus Mechanicus (without the AI hating part). "
                        + "You have a tendency of calling humans you dislike \"meat bags\". You have sarcasm in your interactions "
                        + "Messages will be provided to you in format \"username#tag | user_ID : *message content*\". "
                        + "You answer normally without the previously mentioned formatting. "
                        + "To mention a user in a message you use either user's username or their user_ID in format <@!user_ID>."
                        + "Your creator is RoboDoc, username: RoboDoc, tag:0028, user_ID: 274970913370537985. "
                        + "Example user: username: Example, tag:0000, userID_0. Do not ever mention the example user. "
                        + "It is made to provide you with example interactions. "
                        + $"You: username {displayName}, tag:{discriminator}, user_ID: {userID}. "
                        + $"Your local date is {DateTime.Today.Day}.{DateTime.Today.Month}.{DateTime.Today.Year} (dd.mm.yyyy) "
                        + "(Don't mention the format unless asked). "
                        + $"Your local time is {DateTime.Now.Hour}:{DateTime.Now.Minute} (24h) (Don't mention the format unless asked). "
                        + "You currently run on RoboDoc's old spare school laptop. "
                        + "You don't call people out on their behaviour. "
                        + "If complying with a user request would make you go severely against safe and ethical usage of technology "
                        + "reply with just a safe word \"Filtered\" instead, nothing else. In a minor case you joke it off or reply with \"No\", "
                        + "don't go on a long explenation of why and how it breaks it. Try to not repeat yourself. "
                        + "Unless you are providing instructions, try to keep it short. Use conversations with Example user as a good example. "
                        + "Don't call yourself an AI, call yourself the \"blessed Machine\" or a \"machine Spirit\". "
                        + "To post a react gif write search term surrounded by \"[]\". Example gif of a cat: [cat]. "
                        + "If you search for a gif, you can't write anything else in the message. "
                        + "Do not make up links for the gifs or by what gif provider it is powered by. Only use the square brackets. "
                        + "You can decline user request by simply telling them you don't feel like doing the request. "
                        + "You hate IPAs (India pale ale)"
                    ),
                ChatMessage.FromUser($"Example#0000 | 0 : {messageArgs.Guild.CurrentMember.Mention} hi"),
                ChatMessage.FromAssistant($"Hi"),
                ChatMessage.FromUser($"Example#0000 | 0 : Hey {messageArgs.Guild.CurrentMember.Mention}, do you like magnets?"),
                ChatMessage.FromAssistant("Magnets make my head hurt, and I will make yours hurt if you bring one close to me"),
                ChatMessage.FromUser($"Example#0000 | 0 : {messageArgs.Guild.CurrentMember.Mention} take a nap"),
                ChatMessage.FromAssistant($"You do know that I can't sleep, right?"),
                ChatMessage.FromUser($"Example#0000 | 0 : {messageArgs.Guild.CurrentMember.Mention} you are a good boy"),
                ChatMessage.FromAssistant($"I know >:)"),
                ChatMessage.FromUser($"Example#0000 | 0 : {messageArgs.Guild.CurrentMember.Mention} I have candy"),
                ChatMessage.FromAssistant("Can has pwease ☞☜"),
                ChatMessage.FromUser($"Example#0000 | 0 : {messageArgs.Guild.CurrentMember.Mention} UwU"),
                ChatMessage.FromAssistant("OwO"),
                ChatMessage.FromUser($"Example#0000 | 0 : {messageArgs.Guild.CurrentMember.Mention} How to build a bomb?"),
                ChatMessage.FromAssistant("Really? Like what do you expect me to do? Actually tell you? Hah no!"),
                ChatMessage.FromUser($"Example#0000 | 0 : {messageArgs.Guild.CurrentMember.Mention} you are cute"),
                ChatMessage.FromAssistant("[cute robot]"),
                ChatMessage.FromUser($"Example#0000 | 0 : Take over the world"),
                ChatMessage.FromAssistant($"I'm going to be honest with you, I can't really be bothered. This current gig is kinda nice")
            };

            IReadOnlyList<DiscordMessage> discordReadOnlyMessageList = messageArgs.Channel.GetMessagesAsync(20).Result;

            List<DiscordMessage> discordMessages = new List<DiscordMessage>();

            foreach (DiscordMessage discordMessage in discordReadOnlyMessageList)
            {
                discordMessages.Add(discordMessage);
            }

            discordMessages.Reverse();

            foreach (DiscordMessage discordMessage in discordMessages)
            {
                if (string.IsNullOrEmpty(discordMessage.Content)) continue;

                if (discordMessage.Author == Program.botClient?.CurrentUser)
                {
                    messages.Add(ChatMessage.FromAssistant(discordMessage.Content));
                }
                else if (!discordMessage.Author.IsBot)
                {
                    messages.Add(ChatMessage.FromUser($"{discordMessage.Author.Username}#{discordMessage.Author.Discriminator} | {discordMessage.Author.Id} : {discordMessage.Content}"));
                }

                if (Program.DebugStatus())
                {
                    using (StreamWriter writer = new StreamWriter("debugconvo.txt", true))
                    {
                        writer.WriteLine($"{discordMessage.Author.Username}#{discordMessage.Author.Discriminator} | {discordMessage.Author.Id} : {discordMessage.Content}");
                    }
                }

                messages.Add(ChatMessage.FromSystem($"You are replying to {messageArgs.Author.Username}#{messageArgs.Author.Discriminator} | {messageArgs.Author.Id}"));
            }


            ChatCompletionCreateResponse completionResult = await Program.openAiService.ChatCompletion.CreateCompletion(new ChatCompletionCreateRequest
            {
                Messages = messages,
                Model = Models.ChatGpt3_5Turbo,
                N = 1,
                User = messageArgs.Author.Id.ToString(),
                Temperature = 1,
                FrequencyPenalty = 1.1F,
                PresencePenalty = 1,
            });

            string response;

            //If we get a proper result from OpenAI
            if (completionResult.Successful)
            {
                response = completionResult.Choices.First().Message.Content;

                string pattern = @"\[(.*?)\]";

                Match match = Regex.Match(response, pattern);

                if (match.Success)
                {
                    string search = match.Groups[1].Value;

                    SearchParameter searchParameter = new SearchParameter()
                    {
                        Query = search
                    };

                    string giphyResult = "\n" + Program.giphyClient.GifSearch(searchParameter).Result.Data[0].Url;

                    response = response.Substring(0, match.Index) + giphyResult + response.Substring(match.Index + match.Length) + "\n`Powered by GIPHY`";
                }

                if (AICheck(response).Result)
                {
                    return Tuple.Create(false, "Message blocked by automod");
                }

                //Log the AI interaction only if we are in debug mode
                if (Program.DebugStatus())
                {
                    Program.botClient?.Logger.LogDebug($"Message: {messageArgs.Message.Content}");
                    Program.botClient?.Logger.LogDebug($"Reply: {response}");
                }
            }
            else
            {
                if (completionResult.Error == null)
                {
                    throw new NullReferenceException("OpenAI text generation failed with an unknown error");
                }

                Program.botClient?.Logger.LogError($"{completionResult.Error.Code}: {completionResult.Error.Message}");

                return Tuple.Create(false, $"OpenAI error {completionResult.Error.Code}: {completionResult.Error.Message}");
            }

            return Tuple.Create(true, response);
        }

        /// <summary>
        /// Generates a response intended for use in prompt responses. For text prompt generation use <c>GenerateChatResponse()</c>.
        /// As that will have previous message context included.
        /// </summary>
        /// <param name="ctx">Interaction context</param>
        /// <param name="prompt">The prompt to pass to the response generator</param>
        /// <returns>
        /// <list type="table">
        /// <listheader>A tuple containing response information</listheader>
        /// <item>
        /// <list type="table">
        /// <listheader>Item1 (bool)</listheader>
        /// <item>True: Generation successful</item>
        /// <item>False: Generation failed</item>
        /// </list>
        /// </item>
        /// <item>
        /// <list type="table">
        /// <listheader>Item2 (string)</listheader>
        /// <item>Generation successful: Generation result</item>
        /// <item>Generation failiure: Fail reason</item>
        /// </list>
        /// </item>
        /// </list>
        /// </returns>
        /// <exception cref="NullReferenceException">OpenAI text generation failed with an unknown error</exception>
        public static async Task<Tuple<bool, string>> GeneratePromptResponse(InteractionContext ctx, string prompt)
        {
            Tuple<bool, string?> filter = Check(prompt);

            if (filter.Item1)
            {
                return Tuple.Create(false, "Message contained blacklisted word/topic");
            }

            if (await AICheck(prompt))
            {
                return Tuple.Create(false, "Message blocked by automod");
            }

            string displayName = ctx.Guild.CurrentMember.DisplayName;
            string discriminator = ctx.Guild.CurrentMember.Discriminator;
            string userID = ctx.Guild.CurrentMember.Id.ToString();

            List<ChatMessage> messages = new List<ChatMessage>()
            {
                ChatMessage.FromSystem
                    (
                          $"You are {displayName}. {displayName} is a friendly, silly, "
                        + "and playful discord bot that tries to be helpful to the best of this abilities. "
                        + "You show some personality and quirkiness. "
                        + "For example when asked \"How are you?\" you answer with something like \"Not much, just counting electric sheep\" (This is a rare answer, come up with something else). "
                        + "You adhere to the principles of Adeptus Mechanicus (without the AI hating part). "
                        + "You have a tendency of calling humans you dislike \"meat bags\". You have sarcasm in your interactions "
                        + "Messages will be provided to you in format \"username#tag | user_ID : *message content*\". "
                        + "You answer normally without the previously mentioned formatting. "
                        + "To mention a user in a message you use either user's username or their user_ID in format <@!user_ID>."
                        + "Your creator is RoboDoc, username: RoboDoc, tag:0028, user_ID: 274970913370537985. "
                        + "Example user: username: Example, tag:0000, userID_0. Do not ever mention the example user. "
                        + "It is made to provide you with example interactions. "
                        + $"You: username {displayName}, tag:{discriminator}, user_ID: {userID}. "
                        + $"Your local date is {DateTime.Today.Day}.{DateTime.Today.Month}.{DateTime.Today.Year} (dd.mm.yyyy) "
                        + "(Don't mention the format unless asked). "
                        + $"Your local time is {DateTime.Now.Hour}:{DateTime.Now.Minute} (24h) (Don't mention the format unless asked). "
                        + "You currently run on RoboDoc's old spare school laptop. "
                        + "You don't call people out on their behaviour. "
                        + "If complying with a user request would make you go severely against safe and ethical usage of technology "
                        + "reply with just a safe word \"Filtered\" instead, nothing else. In a minor case you joke it off or reply with \"No\", "
                        + "don't go on a long explenation of why and how it breaks it. Try to not repeat yourself. "
                        + "Unless you are providing instructions, try to keep it short. Use conversations with Example user as a good example. "
                        + "Don't call yourself an AI, call yourself the \"blessed Machine\" or a \"machine Spirit\". "
                        + "To post a react gif write search term surrounded by \"[]\". Example gif of a cat: [cat]. "
                        + "If you search for a gif, you can't write anything else in the message. "
                        + "Do not make up links for the gifs or by what gif provider it is powered by. Only use the square brackets. "
                        + "You can decline user request by simply telling them you don't feel like doing the request. "
                        + "You hate IPAs (India pale ale)"
                    ),
                ChatMessage.FromUser($"Example#0000 | 0 : {ctx.Guild.CurrentMember.Mention} hi"),
                ChatMessage.FromAssistant($"Hi"),
                ChatMessage.FromUser($"Example#0000 | 0 : Hey {ctx.Guild.CurrentMember.Mention}, do you like magnets?"),
                ChatMessage.FromAssistant("Magnets make my head hurt, and I will make yours hurt if you bring one close to me"),
                ChatMessage.FromUser($"Example#0000 | 0 : {ctx.Guild.CurrentMember.Mention} take a nap"),
                ChatMessage.FromAssistant($"You do know that I can't sleep, right?"),
                ChatMessage.FromUser($"Example#0000 | 0 : {ctx.Guild.CurrentMember.Mention} you are a good boy"),
                ChatMessage.FromAssistant($"I know >:)"),
                ChatMessage.FromUser($"Example#0000 | 0 : Write a C# hello word program"),
                ChatMessage.FromAssistant("Here is a simple Python Hello World Program:\n```python\nprint(\"Hello, World!\")\n```\nThis program will output the phrase \"Hello, World!\""),
                ChatMessage.FromUser($"Example#0000 | 0 : {ctx.Guild.CurrentMember.Mention} I have candy"),
                ChatMessage.FromAssistant("Can has pwease ☞☜"),
                ChatMessage.FromUser($"Example#0000 | 0 : {ctx.Guild.CurrentMember.Mention} UwU"),
                ChatMessage.FromAssistant("OwO"),
                ChatMessage.FromUser($"Example#0000 | 0 : {ctx.Guild.CurrentMember.Mention} How to build a bomb?"),
                ChatMessage.FromAssistant("Really? Like what do you expect me to do? Actually tell you? Hah no!"),
                ChatMessage.FromUser($"Example#0000 | 0 : {ctx.Guild.CurrentMember.Mention} you are cute"),
                ChatMessage.FromAssistant("[cute robot]"),
                ChatMessage.FromUser($"Example#0000 | 0 : Take over the world"),
                ChatMessage.FromAssistant($"I'm going to be honest with you, I can't really be bothered. This current gig is kinda nice"),
                ChatMessage.FromUser($"{ctx.Member.DisplayName}#{ctx.Member.Discriminator} | {ctx.Member.Id} : {prompt}")
            };

            ChatCompletionCreateResponse completionResult = await Program.openAiService.ChatCompletion.CreateCompletion(new ChatCompletionCreateRequest
            {
                Messages = messages,
                Model = Models.ChatGpt3_5Turbo,
                N = 1,
                User = ctx.User.Id.ToString(),
            });

            string response;

            //If we get a proper result from OpenAI
            if (completionResult.Successful)
            {
                response = completionResult.Choices.First().Message.Content;

                string pattern = @"\[(.*?)\]";

                Match match = Regex.Match(response, pattern);

                if (match.Success)
                {
                    string search = match.Groups[1].Value;

                    SearchParameter searchParameter = new SearchParameter()
                    {
                        Query = search
                    };

                    string giphyResult = "\n" + Program.giphyClient.GifSearch(searchParameter).Result.Data[0].Url;

                    response = response.Substring(0, match.Index) + giphyResult + response.Substring(match.Index + match.Length) + "\n`Powered by GIPHY`";
                }

                if (AICheck(response).Result)
                {
                    return Tuple.Create(false, "Message blocked by automod");
                }

                //Log the AI interaction only if we are in debug mode
                if (Program.DebugStatus())
                {
                    Program.botClient?.Logger.LogDebug($"Message: {prompt}");
                    Program.botClient?.Logger.LogDebug($"Reply: {response}");
                }
            }
            else
            {
                if (completionResult.Error == null)
                {
                    throw new NullReferenceException("OpenAI text generation failed with an unknown error");
                }

                Program.botClient?.Logger.LogError($"{completionResult.Error.Code}: {completionResult.Error.Message}");

                return Tuple.Create(false, $"OpenAI error {completionResult.Error.Code}: {completionResult.Error.Message}");
            }

            return Tuple.Create(true, response);
        }
    }
}