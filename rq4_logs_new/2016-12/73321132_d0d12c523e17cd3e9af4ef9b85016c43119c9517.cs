using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TennisKata
{
    public class PlayerBuilder
    {
        private string playerName;
        private int numberOfSets;
        private int numberOfGames;
        private int numberOfPoints;

        public PlayerBuilder WithName(string playerName)
        {
            this.playerName = playerName;
            return this;
        }
        public PlayerBuilder WithSets(int numberOfSets)
        {
            this.numberOfSets = numberOfSets;
            return this;

        }
        public PlayerBuilder WithGames(int numberOfGames)
        {
            this.numberOfGames = numberOfGames;
            return this;
        }
        public PlayerBuilder WithPoints(int numberOfPoints)
        {
            this.numberOfPoints = numberOfPoints;
            return this;
        }

        public Player Create()
        {
            return new Player(playerName)
            {
                CurrentSetScore = numberOfGames,
                CurrentGameScore = numberOfPoints,
                CurrentMatchScore = numberOfSets
            };
        }
    }
}