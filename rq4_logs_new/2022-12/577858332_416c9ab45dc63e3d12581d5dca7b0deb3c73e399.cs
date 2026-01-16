using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
using UnityEngine;

[System.Serializable]
public class CardsArtLoader
{
    public async Task SetArtTo(CardData[] cards)
    {
        List<CardData> uniqueCards = FilterUniqueCards(cards);

        Task<Sprite>[] requests = new Task<Sprite>[uniqueCards.Count];

        for (int i = 0; i < requests.Length; i++)
        {
            requests[i] = Picsum.GetSprite();
        }

        await Task.WhenAll(requests);

        for (int i = 0; i < requests.Length && i < uniqueCards.Count; i++)
        {
            uniqueCards[i].art = requests[i].Result;
        }
    }

    private List<CardData> FilterUniqueCards(CardData[] cards)
    {
        List<CardData> uniqueCardData = new List<CardData>();

        for (int i = 0; i < cards.Length; i++)
        {
            if (!uniqueCardData.Contains(cards[i]))
            {
                uniqueCardData.Add(cards[i]);
            }
        }

        return uniqueCardData;
    }
}

public class CardInLoad
{
    public CardData card;
    public bool loading;
}
