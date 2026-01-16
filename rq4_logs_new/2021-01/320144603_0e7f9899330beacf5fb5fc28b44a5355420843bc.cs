using System;
using System.Linq;
using Games.Eucre.Api.Data.Models;
using Games.Eucre.Api.Enums;
using Games.Eucre.Api.Extensions;

namespace Games.Eucre.Api.Data
{
	public static class Extensions
	{
		public static bool IsBot(this Player player)
		{
			return player.User is null;
		}

		public static Card GetConvertedValue(this Card card, Suit trump)
		{
			return card.Value switch
			{
				1 //Ace is worth 14 instead of 1
					=> card with { Value = 14 },
				11 when card.Suit == trump //Right Bauer
					=> card with { Value = 16 },
				11 when card.Suit.IsSameColor(trump) //Left Bauer
					=> new Card(trump, 15),
				_ //All others
					=> card
			};
		}

		public static int GetTeamPoints(this Game game, Team team)
		{
			return team switch
			{
				Team.Red => game.Rounds.Sum(x => x.RedTeamPoints),
				Team.Black => game.Rounds.Sum(x => x.BlackTeamPoints),
				_ => throw new Exception()
			};
		}

		public static Team GetTeam(this Player player)
		{
			return (Team)((int)player.Number % 2);
		}
	}
}