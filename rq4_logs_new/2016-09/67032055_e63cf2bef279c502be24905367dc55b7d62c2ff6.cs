using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NemesesGame
{
    public class Game
    {
        private long chatId;

		private int playerCount = 0;

		public Dictionary<long, string> players = new Dictionary<long, string>();

        public Game()
        {

        }

        public string GameHosted()
        {
            return "New game is made in this lobby!";
        }

		public void PlayerJoin(long playerId, string playerName)
		{
			players.Add(playerId, playerName);
			playerCount++;
		}

		public int PlayerCount { get { return playerCount; } }
    }
}