using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Channels;

namespace FullChecker
{

    public static class IntExtension
    {
        public static T ToEnum<T>(this int enumValue)
        {
            if (Enum.IsDefined(typeof(T), enumValue))
            {
                return (T) (object) enumValue;
            }

            throw new ArgumentException("Enumerable value not found!", nameof(enumValue));
        }
    }
    public static class StringExtension
    {
        public static IEnumerable<string> SplitHandIntoStringCards(this string stringValue)
        {
            if (string.IsNullOrEmpty(stringValue))
            {
                throw new ArgumentException("Invalid string for splitting into cards!", nameof(stringValue));
            }

            return stringValue.Split(" ");
        }

        public static int ConvertToIntRepresentation(this string stringValue)
        {
            if (int.TryParse(stringValue, out var intResult))
            {
                return intResult;
            }

            if (stringValue.Length == 1)
            {
                return stringValue[0];
            }

            throw new ArgumentException("Cannot convert to int representation!", nameof(stringValue));
        }

        public static Card ToCard(this string stringValue)
        {
            if (stringValue.Length < 2 || stringValue.Length > 3)
            {
                throw new ArgumentException("Cannot parse string to card tuple!", nameof(stringValue));
            }

            var cardValue = string.Concat(stringValue.SkipLast(1));
            var cardColor = stringValue.Last();
            if (Enum.IsDefined(typeof(CardValue), cardValue.ConvertToIntRepresentation()) &&
                Enum.IsDefined(typeof(CardColor), (int)cardColor))
            {
                return new Card()
                {
                    Color = ((int) cardColor).ToEnum<CardColor>(),
                    Value = cardValue.ConvertToIntRepresentation().ToEnum<CardValue>()
                };
            }

            throw new ArgumentException("Cannot convert to card!", nameof(stringValue));
        }
    }
    public enum CardColor
    {
        Spades = '♠',
        Hearts = '♥',
        Diamonds = '♦',
        Clubs = '♣'
    }

    public enum CardValue
    {
        Two = 2,
        Three = 3,
        Four = 4,
        Five = 5,
        Six = 6,
        Seven = 7,
        Eight = 8,
        Nine = 9,
        Ten = 10,
        Jack = 'J',
        Queen = 'Q',
        King = 'K',
        Ace = 'A'

    }

    public class Card
    {
        public CardValue Value { get; set; }
        public CardColor Color { get; set; }

        public override string ToString()
        {
            return $"{Value} of {Color}";
        }
    }

    public class Hand
    {
        public IEnumerable<Card> Cards { get; set; }

        public bool HasFullOnHand()
        {
            if (Cards.Count() == 5)
            {
                var distinctValues = Cards.GroupBy(c => c.Value);
                if (distinctValues.Count() == 2)
                {
                    if (distinctValues.Any(g => g.Count() == 2))
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        public override string ToString()
        {
            var cards = Cards.ToList().Select(c => $"  {c},\n");
            var cardsString = string.Concat(cards);
            return string.Concat("[\n", cardsString, "]");
        }
    }

    class TableSupervisor
    {
        public IEnumerable<Hand> Hands { get; set; }

        public void LoadHandsFromFile(string path)
        {
            var lines = File.ReadAllText(path).Split(';');
            var stringHands = lines.ToList().Select(l => l.SplitHandIntoStringCards());
            Hands = stringHands.ToList().Select(sh => sh.ToList().Select(c => c.ToCard())).Select(e => new Hand()
            {
                Cards = e
            });
        }

        public IEnumerable<Hand> GetHandsHavingFull()
        {
            return Hands.Where(h => h.HasFullOnHand());
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length == 1 && (args[0].Equals("-h") || args[0].Equals("--help")))
            {
                Console.WriteLine("Pass input file");
                Console.WriteLine("Example: FullChecker cards.txt");
            }
            else if (args.Length < 1)
            {
                Console.WriteLine("You need to pass the input file as argument!");
                Console.WriteLine("Correct usage: FullChecker <path>");
            }
            else
            {
                var supervisor = new TableSupervisor();
                supervisor.LoadHandsFromFile("cards.txt");
                var fullHands = supervisor.GetHandsHavingFull();
                Console.WriteLine("Hands that have Full:");
                fullHands.Where(h => h.HasFullOnHand()).ToList().ForEach(Console.WriteLine);
            }
        }
    }
}