using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Telegram.Bot;
using Telegram.Bot.Args;
using Telegram.Bot.Types;
using Telegram.Bot.Types.Enums;
using ConsoleApplication2;

namespace ConsoleApplication2
{
    class Program
    {

        public static Dictionary<long, Game> GameDict = new Dictionary<long, Game>();
            
        private static readonly TelegramBotClient Bot = new TelegramBotClient("242212370:AAF2psk3nA3F1Q78rJTVpGQbb7fryiEBl9Q");

        static void Main(string[] args)
        {
            var me = Bot.GetMeAsync().Result;
            Bot.OnMessage += BotOnMessageReceived;
            Bot.StartReceiving();
            Console.ReadLine();
            Bot.StopReceiving();
        }

        private static async void BotOnMessageReceived(object sender, MessageEventArgs messageEventArgs)
        {
            var message = messageEventArgs.Message;
            var messageText = message.Text;
            var chatId = message.Chat.Id;
            var firstName = message.From.FirstName;
            var lastName = message.From.LastName;
            string commandType = "";

            Console.WriteLine("Message received from " + message.From.FirstName + " at " + chatId);
            try
            {
                var a = message.Entities.ElementAt(0).Type;
                commandType = a.ToString();
                Console.WriteLine("commandType: " + commandType);

            } catch (Exception e) { }

            switch (commandType)
            {
                case "BotCommand":
                    await Bot.SendTextMessageAsync(chatId, BotCommandReply(messageText, chatId));                    
                    break;
                default:
                    await Bot.SendTextMessageAsync(chatId, "<put command info here>");
                    break;
            }
            
            Console.WriteLine("Reply has been sent!");
        }

        private static string BotCommandReply (string commandString, long chatId)
        {
            string s;
            switch(commandString)
            {
                case "/NewGame":
                    s = "New Game command unimplemented yet!";
                    if (GameDict.ContainsKey(chatId)) {
                        s = "Game already running!";
                    } else {
                        GameDict.Add(chatId, new Game());
                        s = GameDict[chatId].IsItRunning();
                    }
                    break;
                default:
                    s = "Command not found!";
                    break;
            }
            return s;
        }
    }
}

