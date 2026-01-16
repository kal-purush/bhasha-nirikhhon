using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AIPokerPlayer.Poker
{
    // Written by Alex Ciaramella
    class EvalResult
    {
        int highCard;
        int handValue;

        public EvalResult(int highCard, int handValue)
        {
            this.highCard = highCard;
            this.handValue = handValue;
        }

        public void setHighCard(int highCard)
        {
            this.highCard = highCard;
        }

        public void setHandValue(int handValue)
        {
            this.handValue = handValue;
        }
    }
}