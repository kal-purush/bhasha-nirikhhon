using System.Collections.Generic;

namespace BlackJackTeamProject.Models
{
	public class Game
	{
		public static List<Player> Players { get; set; }

		public void StartGame()
		{
			// TODO: Randomize deck
			// TODO: Deal cards
			// TODO: Set player turn (for mvp just set dealer or player)
		}

		public void Hold()
		{
			// TODO: End player's turn
		}

		public void Hit()
		{
			// TODO: Give player new card
			// TODO: Update player's score
			// TODO: Check player score for > 21
			// TODO: Player's turn continues if < 21
		}

		public void Bust()
		{
			// TODO: End player's turn
			// TODO: Player gets no points
		}

		public void EndGame()
		{
			// TODO: End all turns (if not already)
			// TODO: Show final results
			// TODO: Allow starting new game
		}
	}
}