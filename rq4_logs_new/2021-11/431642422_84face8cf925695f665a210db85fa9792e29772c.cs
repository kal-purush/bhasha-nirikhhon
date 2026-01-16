using System;
using System.ServiceModel.Channels;
using System.Threading.Tasks;
using Telegram.Bot;
using Telegram.Bot.Types;
using Telegram.Bot.Types.ReplyMarkups;
using Telegram.Bot.Args;
using Message = Telegram.Bot.Types.Message;

namespace myBD
{
    internal class BotHandlers
    {
        public BotHandlers(ITelegramBotClient _botclient)
        {
            botClient = _botclient;
            order = new Order();
        }
        private ITelegramBotClient botClient;
        private Order order;
        public async void botClient_OnMessage(object sender, Telegram.Bot.Args.MessageEventArgs e)
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
                    order.applesCount++;
                    var message = AddItemToOrder(e.Message);
                }
                else if (e.Message.Text.ToLower().Contains("вишня"))
                {
                    order.grapesCount++;
                    var message = AddItemToOrder(e.Message);
                }
                else if (e.Message.Text.ToLower().Contains("слива"))
                {
                    order.plumsCount++;
                    var message = AddItemToOrder(e.Message);
                }
                else if (e.Message.Text.ToLower().Contains("груша"))
                {
                    order.pearsCount++;
                    var message = AddItemToOrder(e.Message);
                }
                else if (e.Message.Text.ToLower().Contains("статистика") || e.Message.Text.ToLower().Contains("статистику"))
                {
                    await botClient.SendTextMessageAsync(chatId: e.Message.Chat.Id,
                        text: $"Всього замовлень: 3");
                }
                else if (e.Message.Text.ToLower().Contains("корзина") || e.Message.Text.ToLower().Contains("корзину"))
                {
                    int count = order.pearsCount + order.plumsCount + order.grapesCount + order.applesCount;
                    string text = $"Товарів в корзині: {count}\n" +
                        $"Яблука : {order.applesCount} шт\n" +
                        $"Вишні : {order.grapesCount} шт\n" +
                        $"Сливи : {order.plumsCount} шт\n" +
                        $"Груші : {order.pearsCount} шт\n";
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

        public async Task<Message> Usage(Telegram.Bot.Types.Message message)
        {
            const string usage = "Використовуй ^_^:\n" +
                                 "/keyboard - показати клавіатуру\n" +
                                 "/remove   - приховати клавіатуру\n";

            return await botClient.SendTextMessageAsync(chatId: message.Chat.Id,
                                                        text: usage,
                                                        replyMarkup: new ReplyKeyboardRemove());
        }

        public async Task<Message> SendReplyKeyboard(Message message)
        {
            ReplyKeyboardMarkup replyKeyboardMarkup = new ReplyKeyboardMarkup(
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

        public async Task<Message> RemoveKeyboard(Message message)
        {
            return await botClient.SendTextMessageAsync(chatId: message.Chat.Id,
                                                        text: "Клавіатуру приховано",
                                                        replyMarkup: new ReplyKeyboardRemove());
        }

        public async Task<Message> AddItemToOrder(Message message)
        {
            ReplyKeyboardMarkup replyKeyboardMarkup = new ReplyKeyboardMarkup(
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
        public int applesCount { get; set; } = 0;
        public int grapesCount { get; set; } = 0;
        public int plumsCount { get; set; } = 0;
        public int pearsCount { get; set; } = 0;
    }
}