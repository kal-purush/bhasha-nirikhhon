using System;
using System.Collections.Generic;
using System.Linq;

namespace Black_Jack_Game
{
    public class Deck
    {
        
        //Deck.Generate Deck of Cards -
        //Deck.Shuffle deck of cards - to randomize
        //Deck.DrawCard() - return top card from shuffled deck of cards
        
        public Deck()
        {
            
        }

        public List<string> GenerateDeck()
        {
        
            List<string> _completeDeck = new List<string>();
            List<string> _suites = new List<string>() {"Clubs", "Diamonds", "Hearts", "Spades"};
            List<string> _values = new List<string>() {"2", "3", "4", "5", "6", "7", "8", "9", "10", "Jack", "King", "Queen", "Ace"};

            for (int i = 0; i < _suites.Count; i++)
            {
                for (int j = 0; j < _values.Count; j++)
                {
                    /*string cardValue = "";
                    
                    switch (values[j])
                    {
                        case (10):
                            cardValue = "Ace";
                            break;
                        case (13):
                            cardValue = "King";
                            break;
                        case (12):
                            cardValue = "Queen";
                            break;
                        case (11):
                            cardValue = "Jack";
                            break;
                        default:
                            cardValue = values[j].ToString();
                            break;
                    }*/
                    
                    string card = _suites[i] + _values[j];
                    _completeDeck.Add(card);
                }
            }
            Console.WriteLine("\ncompleted deck:");
            for (int i = 0; i <_completeDeck.Count; i++)
            {
                Console.WriteLine(_completeDeck[i]);
            }
            //completeDeck.ForEach(Console.WriteLine);
            //Console.Write(completeDeck.Count);
            //Shuffle(completeDeck);

            return _completeDeck;
        }

        public List<string> Shuffle(List<string> completeDeck)
        {
            List<string> _shuffledDeck = completeDeck.OrderBy(x => Guid.NewGuid()).ToList();
            Console.WriteLine("\nshuffled deck:");
            _shuffledDeck.ForEach(Console.WriteLine);
            //DrawCard(shuffledDeck);
            return _shuffledDeck;
        }

        public string DrawCard(List<string> shuffledDeck)
        {
            String _firstCard = shuffledDeck[0];
            shuffledDeck.RemoveAt(0);
            Console.WriteLine("\ntopcard: " + _firstCard);
            return _firstCard;
        }
        
    }
}