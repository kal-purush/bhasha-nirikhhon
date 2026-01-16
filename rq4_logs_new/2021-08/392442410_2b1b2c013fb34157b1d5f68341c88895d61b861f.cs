using System.Collections.Generic;
using System.Linq;

namespace  BlackJackTeamProject.Models
{
	public class Logic
	{
		public List<Player> Players { get; set;}

		// Adds the given score to the player's current score
		public void UpdateRoundScoreForPlayer(Player player, float score)
		{
			player.RoundScore += score;
		}

		// Returns a Player that is the winner for the current round
		public List<Player> GetWinner()
		{
			Dictionary<Player, float> allPlayerScores = new Dictionary<Player, float>();
			List<Player> playersWithTopScore = new List<Player>();
			float topScore;

			// Iterates through each player to get all scores
			for (int i = 0; i < Players.Count; i++)
			{
				Player player = Players[i];
				if (player.State != Player.PlayerState.Lost)
				{
					allPlayerScores.Add(player, player.RoundScore);
				}
			}

			// Sets the top score
			topScore = allPlayerScores.Values.Max();

			// Iterates through each score to determine which players scored the highest
			foreach (KeyValuePair<Player, float> kvp in allPlayerScores)
			{
				if (kvp.Value  >= topScore) playersWithTopScore.Add(kvp.Key);
			}

			return playersWithTopScore;
		}

		// Returns a bool for if the given player has lost or not
		public bool HasPlayerLost(Player player)
		{
			bool hasLost = player.RoundScore > 21;

			// Set player lost state to not allow them to continue playing
			if (hasLost) player.State = Player.PlayerState.Lost;

			return hasLost;
		}
	}
}