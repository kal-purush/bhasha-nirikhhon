using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Telegram.Bot;
using Telegram.Bot.Types;
using Telegram.Bot.Types.ReplyMarkups;

namespace TelegramBot
{
    //git remote add origin https://github.com/IvanitskyiVolodymyr/Ivanitskyi_Volodymyr.git
   // git pull origin master

    class BotHandlers
    {
        public BotHandlers(ITelegramBotClient _botclient)
        {
            botClient = _botclient;
        }
        private ITelegramBotClient botClient;
        public  async void botClient_OnMessage(object sender, Telegram.Bot.Args.MessageEventArgs e)
        {
            if (e.Message != null &&
                e.Message.Type == Telegram.Bot.Types.Enums.MessageType.Text &&
                !string.IsNullOrEmpty(e.Message.Text))
            {
                if (e.Message.Text.ToLower().Contains("замовлення"))
                {
                    var message = AddItemToOrder(e.Message);

                }
                else if (e.Message.Text.ToLower().Contains("яблуко"))
                {
                    Order.applesCount++;
                    var message = AddItemToOrder(e.Message);
                }
                else if (e.Message.Text.ToLower().Contains("вишня"))
                {
                    Order.grapesCount++;
                    var message = AddItemToOrder(e.Message);
                }
                else if (e.Message.Text.ToLower().Contains("слива"))
                {
                    Order.plumsCount++;
                    var message = AddItemToOrder(e.Message);
                }
                else if (e.Message.Text.ToLower().Contains("груша"))
                {
                    Order.pearsCount++;
                    var message = AddItemToOrder(e.Message);
                }
                else if (e.Message.Text.ToLower().Contains("статистика") || e.Message.Text.ToLower().Contains("статистику"))
                {
                    await botClient.SendTextMessageAsync(chatId: e.Message.Chat.Id,
                        text: $"Всього замовлень: 3");
                }
                else if (e.Message.Text.ToLower().Contains("корзина") || e.Message.Text.ToLower().Contains("корзину"))
                {
                    int count = Order.pearsCount + Order.plumsCount + Order.grapesCount + Order.applesCount;
                    string text = $"Товарів в корзині: {count}\n" +
                        $"Яблука : {Order.applesCount} шт\n" +
                        $"Вишні : {Order.grapesCount} шт\n" +
                        $"Сливи : {Order.plumsCount} шт\n" +
                        $"Груші : {Order.pearsCount} шт\n";
                    await botClient.SendTextMessageAsync(chatId: e.Message.Chat.Id,
                        text: text,
                        replyMarkup: new ReplyKeyboardRemove()
                        );
                }
                else if (e.Message.Text.ToLower() == "/keyboard")
                {
                    var message = SendReplyKeyboard(e.Message);
                }
                else if (e.Message.Text.ToLower() == "/remove")
                {
                    var mesage = RemoveKeyboard(e.Message);
                }
                else
                {
                    var message = Usage(e.Message);
                    Console.WriteLine(message.Result.Text);
                }
            }
        }

        public  async Task<Message> Usage(Message message)
        {
            const string usage = "Використовуй ^_^:\n" +
                                 "/keyboard - показати клавіатуру\n" +
                                 "/remove   - приховати клавіатуру\n";

            return await botClient.SendTextMessageAsync(chatId: message.Chat.Id,
                                                        text: usage,
                                                        replyMarkup: new ReplyKeyboardRemove());
        }

        public  async Task<Message> SendReplyKeyboard(Message message)
        {
            ReplyKeyboardMarkup replyKeyboardMarkup = new(
                new[]
                {
                        new KeyboardButton[] { "Статистика"},
                        new KeyboardButton[] { "Добавити замовлення" },
                })
            {
                ResizeKeyboard = true
            };

            return await botClient.SendTextMessageAsync(chatId: message.Chat.Id,
                                                        text: "Обирай",
                                                        replyMarkup: replyKeyboardMarkup);
        }

        public  async Task<Message> RemoveKeyboard(Message message)
        {
            return await botClient.SendTextMessageAsync(chatId: message.Chat.Id,
                                                        text: "Клавіатуру приховано",
                                                        replyMarkup: new ReplyKeyboardRemove());
        }

        public async Task<Message> AddItemToOrder(Message message)
        {
            ReplyKeyboardMarkup replyKeyboardMarkup = new(
                new[]
                {
                        new KeyboardButton[] { "Яблуко", "Вишня"},
                        new KeyboardButton[] { "Слива", "Груша" },
                        new KeyboardButton[] { "Перейти в корзину" }
                })
            {
                ResizeKeyboard = true
            };

            return await botClient.SendTextMessageAsync(chatId: message.Chat.Id,
                                                        text: "Обирай фрукт",
                                                        replyMarkup: replyKeyboardMarkup);
        }
    }
    class Order
    {
        public static  int applesCount { get; set; } = 0;
        public static int grapesCount { get; set; } = 0;
        public static int plumsCount { get; set; } = 0;
        public static int pearsCount { get; set; } = 0;
    }
}