using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Lab2
{
    public class Game
    {
        public string Team1;
        public string Team2;
        public int Team1Score;
        public int Team2Score;

        public Game()
        {

        }

        public Game(string Team1, string Team2, int Team1Score, int Team2Score)
        {
            this.Team1 = Team1;
            this.Team2 = Team2;
            this.Team1Score = Team1Score;
            this.Team2Score = Team2Score;
        }

        public override string ToString ()
        {
            return Team1 + " (" + Team1Score + ") - " + Team2 + " (" + Team2Score + ")";
        }
    }
}