using System;

namespace DeckOfCards
{
    public class Card
    {
        public enum Suit
        {
            Hearts, Spades, Diamonds, Clubs
        }

        public enum FaceValue
        {
            Ace, Two, Three, Four, Five, Six, Seven, Eight, Nine, Ten, Jack, Queen, King
        }

        Suit suit;
        FaceValue faceValue;

        public Card(Suit mySuit, FaceValue myFaceValue)
        {
            suit = mySuit;
            faceValue = myFaceValue;
        }

        public void printCard()
        {
            Console.WriteLine(faceValue.ToString() + " of " + suit.ToString());
        }
    }
}