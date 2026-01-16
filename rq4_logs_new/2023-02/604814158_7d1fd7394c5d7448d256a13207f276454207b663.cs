using Casino.data.model;
using Casino.data.model.card;

namespace Casino.game;

public static class GameDeckFactory
{
    public static Deck Create()
    {
        var cards = new List<Card>();
        var suits = Enum.GetValues(typeof(CardSuit)).Cast<CardSuit>().ToList();
        var ranks = Enum.GetValues(typeof(CardRank)).Cast<CardRank>().ToList();
        foreach (var cardRank in ranks)
        {
            cards.AddRange(suits.Select(cardSuit => new Card(cardSuit, cardRank)));
        }

        return new Deck(cards);
    }
}