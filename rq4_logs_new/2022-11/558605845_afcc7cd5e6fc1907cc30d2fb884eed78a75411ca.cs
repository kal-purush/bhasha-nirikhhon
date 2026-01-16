using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using Domain;

namespace Services
{
    [DataContract]
    public class GamePlayerQueue
    {
        public Queue<GamesPlayers> queue = new Queue<GamesPlayers>();
        private static Random randomGenerator = new Random();
        public void Initialize(List<GamesPlayers> gamesPlayers)
        {
            gamesPlayers = Shuffle(gamesPlayers);
            foreach (GamesPlayers gamePlayer in gamesPlayers)
            {
                this.queue.Enqueue(gamePlayer);
                
            }
            
        }

        public void Cycle()
        { 
            GamesPlayers gamesPlayers = this.queue.Dequeue();
            this.queue.Enqueue(gamesPlayers);
        }

        private List<GamesPlayers> Shuffle(List<GamesPlayers> gamesPlayers)
        {
            int gamesPlayersCount = gamesPlayers.Count;
            while (gamesPlayersCount > 1)
            {
                gamesPlayersCount--;
                int nextGamePlayer = randomGenerator.Next(gamesPlayersCount + 1);
                GamesPlayers gamePlayer = gamesPlayers[nextGamePlayer];
                gamesPlayers[nextGamePlayer] = gamesPlayers[gamesPlayersCount];
                gamesPlayers[gamesPlayersCount] = gamePlayer;
            }
            return gamesPlayers;
        }

    }
}