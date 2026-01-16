using Discord;
using DiscordBot.Util.Commands;
using DiscordBot.Util.Commands.Attributes;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace KlausPeterBot.BotCommands
{
	[CommandVerb("gti", Description = "Shows the remaining time to the GTI", Hidden = true)]
	public sealed class GtiCountdownCommand : ICommand
	{
		private static readonly Random _random = new Random();
		private static readonly DateTime _deliveryDate = new DateTime(2019, 10, 11);

		public async Task Execute(IMessage rawMessage)
		{
			bool addGif = _random.Next(0, 1000) < 100;
			TimeSpan remaining = _deliveryDate.Subtract(DateTime.UtcNow);
			int dayRounded = Convert.ToInt32(remaining.TotalDays);

			if (dayRounded > 0)
			{
				await rawMessage.Channel.SendMessageAsync($"Noch ~{dayRounded} Tage bis zur Höllenmaschine :race_car:");
			}
			else
			{
				await rawMessage.Channel.SendMessageAsync($":confetti_ball: :tada: Es ist soweit :tada: :confetti_ball:");
			}
		}
	}
}