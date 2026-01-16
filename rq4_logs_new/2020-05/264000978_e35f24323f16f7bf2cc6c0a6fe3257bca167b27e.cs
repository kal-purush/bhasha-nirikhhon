using System;
using System.Collections.Generic;

namespace DeckOfCards
{
    class Deck
    {
        public List<Card> deckOfCards = new List<Card>();//List for 52 Cards, currently empty

        //public currentCard;                            //Card to be dealt/drawn

        public void init()
        {
            foreach (Card.Suit suit in Enum.GetValues(typeof(Card.Suit)))
            {
                foreach (Card.FaceValue faceValue in Enum.GetValues(typeof(Card.FaceValue)))
                {
                    deckOfCards.Add(new Card(suit, faceValue));
                }
            }
        }
        public Card draw()
        {
            //draw a Card, from Deck, remove it from Deck
            Card card = deckOfCards[0];
            deckOfCards.RemoveAt(0);
            return card;
        }
        public void shuffle()   //does this need a parameter?
        {
            //loop to shuffle
            List<Card> newDeck = new List<Card>();
            Random random = new Random();
            while (deckOfCards.Count > 0)
            {
                int index = random.Next(deckOfCards.Count);
                Card card = deckOfCards[index];
                deckOfCards.RemoveAt(index);
                newDeck.Add(card);
            }

            deckOfCards = newDeck;
        }
        public string ReturnMessage()
        {
            return "Deck has been shuffled";
        }
    }   
}