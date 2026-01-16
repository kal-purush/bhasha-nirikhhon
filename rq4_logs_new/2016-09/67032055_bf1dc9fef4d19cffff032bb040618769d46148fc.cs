using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Telegram.Bot.Types;
using Telegram.Bot.Types.Enums;
using Telegram.Bot.Types.ReplyMarkups;

namespace NemesesGame
{
    public class CityChatHandler
    {
        public string privateReply = "";
        public List<InlineKeyboardButton> buttons = new List<InlineKeyboardButton>();
        public InlineKeyboardMarkup menu;

        public long playerId;
        public int msgId = 0;
        
        public Stack<InlineKeyboardMarkup> menuHistory = new Stack<InlineKeyboardMarkup>(10);
        public Stack<string> replyHistory = new Stack<string>(10);

        public CityChatHandler(long _playerId)
        {
            playerId = _playerId;
        }

        
        public void AddMenuButton(InlineKeyboardButton button)
        {
            buttons.Add(button);
            //Console.WriteLine(button.CallbackData);
        }

        public void SetMenu()
        {
            menu = new InlineKeyboardMarkup(buttons.Select(x => new[] { x }).ToArray());
            //Console.WriteLine()
        }

        /// <summary>
        /// Saves reply history for 'Back' button
        /// </summary>
        /// <param name="menu">InlineKeyboardMarkup to save</param>
        /// <param name="reply">Reply string to save</param>
        public void AddReplyHistory()
        {
            menuHistory.Push(menu);
            replyHistory.Push(privateReply);
        }

        public void ClearReplyHistory()
        {
            menuHistory.Clear();
            replyHistory.Clear();
        }
        public void AddReply(string replyString)
        {
            privateReply += replyString;
        }

        public async Task SendReply(ParseMode _parseMode = ParseMode.Markdown)
        {
            string replyString = privateReply;

            privateReply = "";
            if (buttons != null && menu != null)
            {
                await Program.SendMessage(playerId, replyString, menu, _parseMode);

                buttons.Clear();
                menu = null;
            }
            else
            {
                await Program.SendMessage(playerId, replyString, _parseMode: _parseMode);
            }
        }

        public async Task EditMessage(IReplyMarkup replyMarkup = null, ParseMode ParseMode = ParseMode.Markdown)
        {
            string replyString = privateReply;

            privateReply = "";
            await Program.EditMessage(playerId, msgId, replyString, repMarkup: menu, _parseMode: ParseMode);
            replyString = "";

            if (buttons != null)
            {
                buttons.Clear();
                menu = null;
            }
        }
    }
}