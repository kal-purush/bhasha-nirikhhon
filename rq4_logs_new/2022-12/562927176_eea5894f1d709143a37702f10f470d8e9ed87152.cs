using System;
using TeleSharp.TL;
using TLSharp.Core;
using TLSharp;
using Telegram.Bot;
using Telegram.Bot.Exceptions;
//using Telegram.Bot.Extensions.Polling;
using Telegram.Bot.Types;
using Telegram.Bot.Types.Enums;
using System.Threading;
using System.Threading.Tasks;
using TelegramClient = TLSharp.Core.TelegramClient;

namespace TvoiShop.Telegram.Bot
{
    public class BotManager
    {
        private TelegramClient _client;

        static string Config(string what)
        {
            switch (what)
            {
                case "api_id": return "20790770";
                case "api_hash": return "b71211aa7cf9d79bf41d1b8623c668a6";
                case "phone_number": return "+380977903314";
                case "verification_code":
                    var code = "a";
                    return code;
                case "first_name": return "John";      // if sign-up is required
                case "last_name": return "Doe";        // if sign-up is required
                case "password": return "secret!";     // if user has enabled 2FA
                default: return null;                  // let WTelegramClient decide the default config
            }
        }

        public async void Connect(int apiId, string apiHash)
        {
            /*_client = new TelegramClient(apiId, apiHash);
            await _client.ConnectAsync();
            var hash = await _client.SendCodeRequestAsync("380977903314");

            var code = "<code_from_telegram>";

            var user = await _client.MakeAuthAsync("380977903314", hash, code);

            var users = await _client.GetContactsAsync();

            int chatId = 449703652;

            await _client.SendMessageAsync(new TLInputPeerUser() { UserId = chatId }, "Hello baybe!");*/

            using var client = new WTelegram.Client(Config);
            var myself = await client.LoginUserIfNeeded();
            var a = myself;
            /*TelegramBotClient client = new TelegramBotClient("5377841386:AAEs9bs462z4R29IZI73hRej2jPFCmTUeDo");

            var currentUser = await client.GetMeAsync();

            using var cts = new CancellationTokenSource();

            // StartReceiving does not block the caller thread. Receiving is done on the ThreadPool.
            var receiverOptions = new ReceiverOptions
            {
                AllowedUpdates = { } // receive all update types
            };
            client.StartReceiving(
                HandleUpdateAsync,
                HandleErrorAsync,
                receiverOptions,
                cancellationToken: cts.Token);

            var me = await client.GetMeAsync();

            Console.WriteLine($"Start listening for @{me.Username}");
            Console.ReadLine();

            // Send cancellation request to stop bot
            cts.Cancel();*/

        }
        async Task HandleUpdateAsync(ITelegramBotClient botClient, Update update, CancellationToken cancellationToken)
        {
            // Only process Message updates: https://core.telegram.org/bots/api#message
            if (update.Type != UpdateType.Message)
                return;
            // Only process text messages
            if (update.Message!.Type != MessageType.Text)
                return;

            var chatId = update.Message.Chat.Id;
            var messageText = update.Message.Text;

            Console.WriteLine($"Received a '{messageText}' message in chat {chatId}.");

            // Echo received message text
            Message sentMessage = await botClient.SendTextMessageAsync(
                chatId: chatId,
                text: "You said:\n" + messageText,
                cancellationToken: cancellationToken);
        }

        Task HandleErrorAsync(ITelegramBotClient botClient, Exception exception, CancellationToken cancellationToken)
        {
            var ErrorMessage = exception switch
            {
                ApiRequestException apiRequestException
                    => $"Telegram API Error:\n[{apiRequestException.ErrorCode}]\n{apiRequestException.Message}",
                _ => exception.ToString()
            };

            Console.WriteLine(ErrorMessage);
            return Task.CompletedTask;
        }
    }
}