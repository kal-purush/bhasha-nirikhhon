using Discord.Commands;
using Discord.WebSocket;
using System.Reflection;
using System.Threading.Tasks;

namespace Botelek1_CSharp.Core
{
    class CommandHandler
    {
        DiscordSocketClient _client;
        CommandService _commands;

        public async Task InitializeAsync(DiscordSocketClient client)
        {
            _client = client;
            _commands = new CommandService();

            await _commands.AddModulesAsync(Assembly.GetEntryAssembly());
            _client.MessageReceived += HandleCommandAsync;
        }

        private async Task HandleCommandAsync(SocketMessage message)
        {
            SocketUserMessage msg = message as SocketUserMessage;

            if (msg == null) return;

            SocketCommandContext context = new SocketCommandContext(_client, msg);

            int argPos = 0;

            if (msg.HasStringPrefix(Config.bot.CmdPrefix, ref argPos) || msg.HasMentionPrefix(_client.CurrentUser, ref argPos))
            {
                IResult result = await _commands.ExecuteAsync(context, argPos);
                if (!result.IsSuccess && result.Error != CommandError.UnknownCommand)
                {
                    await context.Channel.SendMessageAsync($"[ERROR] {result.ErrorReason}");
                }
            }
        }
    }
}