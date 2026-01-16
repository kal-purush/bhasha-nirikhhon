using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WumpusTest
{
    class Trivia
    {
        private Trivia _Trivia;

        public int BottomlessPit()
        {
            int roomNum = 0;
            return roomNum;
            /*
             * When initiated from Game Control, The player will be asked 3 questions
             * each question has four possible responses
             * the player wins if they answer 2/3 questions
             * return roomNumber if won
             * return negative number (Game Over) if lost
             */
        }

        public int WumpusRoom()
        {
            int moveWumpusToRoom = 0;
            return moveWumpusToRoom;
            /*
             * When initiated, The wumpus is in the same room
             * the player is asked 5 questions, each with 4 answers
             * win: majority of questions won; WUmpus moves locations
             * loss: majority lost, game over (return negative number)
             */
        }
    }
}