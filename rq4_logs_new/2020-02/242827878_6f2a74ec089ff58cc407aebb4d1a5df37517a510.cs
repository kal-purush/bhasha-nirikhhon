using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
//using System.File;

namespace HighScore
{
    class HighScore
    {
        private int playerScore;

        public HighScore(int score)
        {
            playerScore = score;
        }
        public int setStartingScore(int score)
        {
            return 0;
            // allows gameControl to determine the player's score at the start.
        }
        public int getScoreAfterTurn(bool turn)
        {

            return 0;
            // this method returns the score after a turn was made in the game.
            // Parameter is from game control. Will determine if a turn was made.
        }
        public int getFinalScore(bool winOrLoss, int coins, int arrows)
        {

            return 0;
            // returns final score using boolean paramter to determine if a win or a loss.
        }
        public String toString()
        {
            String str = "Score: ";
            str += playerScore;
            return str;
            // displays current score
        }
        
        public String showHighScores()
        {
            String str = "High Scores: " + playerScore;
            return str;
        }

    }
}