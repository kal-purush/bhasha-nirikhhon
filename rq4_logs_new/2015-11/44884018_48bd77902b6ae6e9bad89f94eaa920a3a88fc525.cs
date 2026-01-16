using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using AIPokerPlayer.Poker.Cards;

namespace AIPokerPlayer.Players
{
    // Written by Alex Ciaramella
    abstract class Player
    {
        int chipCount; // the player's current chip count
        int positionOnBoard;
        private string name;
        List<Card> playerHand; // the player's two cards held in their hand

        public Player(string name, int startingChipCount, int position)
        {
            this.chipCount = startingChipCount;
            this.name = name;
        }

        // get the player's decision for their next move
        public abstract void requestAction();

        // adds the 2 cards to the player's starting hand
        public void addCardsToHand(List<Card> cards)
        {
            playerHand = new List<Card>(cards);
        }

        // returns the player's chip count
        public int getChipCount()
        {
            return chipCount;
        }

        public string getName()
        {
            return name;
        }

        // increases or decreases the player's chip count depending on if the amount is positive or negative
        public int modifyChipCount(int amount)
        {
            int result = 0;

            // check if this will put our player in negative chips
            if (amount < 0 && chipCount < Math.Abs(amount))
            {
                result = chipCount;
                chipCount = 0;
            }
            else
            {
                chipCount += amount;
                result = chipCount;
            }

            return result;
        }
    }
}