
using System.Collections.Generic;
using System;

namespace RpsGame_NoDb
{
    class Match
    {
        private Guid matchId = new Guid();
        public Guid MatchId { get { return matchId; } }

        public Player Player1 { get; set; } // always the computer
        public Player Player2 { get; set; } // always the user.

        List<Round> Rounds = new List<Round>();

        private int p1RoundWins { get; set; } // ho many rounds has the player won?
        private int p2RoundWins { get; set; }
        private int ties { get; set; }

        public void RoundWinner(Player p = null)
        {
            if (p.PlayerId == Player1.PlayerId)
            {
                p1RoundWins++;
            }
            else if (p.PlayerId == Player2.PlayerId)
            {
                p2RoundWins++;

            }
            else
            {
                ties++;
            }
        }

        public Player MatchWinner()
        {
            if (p1RoundWins == 2)
            {
                return Player1;
            }
            else if (p2RoundWins == 2)
            {
                return Player2;
            }
            else
            {
                return null;
            }
        }





    }


}
}