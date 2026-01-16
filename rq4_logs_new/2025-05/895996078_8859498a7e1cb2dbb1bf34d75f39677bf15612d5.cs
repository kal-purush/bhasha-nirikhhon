using PrenburtisBot.Attributes;
using PrenburtisBot.Types;
using Telegram.Bot;
using Telegram.Bot.Types;
using TelegramBotBase.Base;

namespace PrenburtisBot.Forms
{
	[BotCommand("Остановить опрос", Telegram.Bot.Types.Enums.BotCommandScopeType.AllChatAdministrators)]
	internal class StopPoll : BotCommandGroupFormBase
	{
		public async Task<TextMessage> RenderAsync(long chatId, int messageId)
		{
			await this.Client.TelegramClient.StopPollAsync(chatId, messageId);
			return new TextMessage(string.Empty).NavigateToStart(Start.SET_QUIET);
		}

		public async Task<TextMessage> RenderAsync(MessageResult message)
		{
			if (message.Message.ReplyToMessage is Message replyToMessage && replyToMessage.Poll is Poll poll && !poll.IsClosed)
			{
				return await RenderAsync(replyToMessage.Chat.Id, replyToMessage.MessageId);
			}
			else
				throw new ArgumentException("Команда должна вызываться в ответ на незакрытый опрос", nameof(message));
		}
	}
}