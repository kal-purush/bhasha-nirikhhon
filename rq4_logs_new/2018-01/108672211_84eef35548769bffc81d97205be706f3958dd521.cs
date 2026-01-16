using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Blackjack
{

    public class Card
    {
        public enum Suit { Heartes, Clubs, Diamond, Speades }
        public enum Rank { Ace, Two, Three, Four, Five, Six, Seven, Eight, Nine, Ten, Jack, Queen, King }
        public Suit cardSuit { get; private set; }
        public Rank cardRank { get; private set; }

        public Card(Suit suit, Rank rank)
        {
            this.cardSuit = suit;
            this.cardRank = rank;
        }

        public String GetSuitAsString()
        {
            switch (cardSuit)
            {
                case Card.Suit.Clubs: return "C";
                case Card.Suit.Diamond: return "D";
                case Card.Suit.Heartes: return "H";
                case Card.Suit.Speades: return "S";
            }

            return cardSuit.ToString();
        }

        public String GetRankAsString()
        {

            switch (cardRank)
            {
                case Card.Rank.Ace: return "A";
                case Card.Rank.Two: return "2";
                case Card.Rank.Three: return "3";
                case Card.Rank.Four: return "4";
                case Card.Rank.Five: return "5";
                case Card.Rank.Six: return "6";
                case Card.Rank.Seven: return "7";
                case Card.Rank.Eight: return "8";
                case Card.Rank.Nine: return "9";
                case Card.Rank.Ten: return "10";
                case Card.Rank.Jack: return "J";
                case Card.Rank.Queen: return "Q";
                case Card.Rank.King: return "K";
            }

            return cardRank.ToString();
        }

        public override string ToString()
        {
            return GetRankAsString() + GetSuitAsString();
        }


    }
}