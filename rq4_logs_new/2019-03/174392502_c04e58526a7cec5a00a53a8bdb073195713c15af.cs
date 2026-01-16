using System.Collections;
using System.Collections.Generic;
using UnityEngine;

/// <summary>
/// Controls the card deck
/// Maintains queue of cards to be given to player
/// </summary>
public class CardDeckController : MonoBehaviour
{
    public List<ActivityCard> deck;
    public Queue<ActivityCard> priorityCards;

    // Start is called before the first frame update
    void Start()
    {
        priorityCards = new Queue<ActivityCard>();
    }

    //Public methods
    public void AddToQueue(ActivityCard card)
    {
        priorityCards.Enqueue(card);
    }

    //Gets valid card based on Player's stats, features and history
    public ActivityCard GetCard(int currentTurn, CardIntDictionary choiceHistory, PlayerStatIntDictionary playerStats, List<string> playerFeatures)
    {
        ActivityCard result = null;

        //Check priority queue first
        if(priorityCards.Count > 0)
        {
            result = priorityCards.Dequeue();
        }
        else //otherwise find first valid card from list of all cards
        {
            for(int i = 0; i < deck.Count; i++)
            {
                if (deck[i].ValidateCard(currentTurn, playerFeatures, playerStats, choiceHistory)){
                    result = deck[i];
                    deck.RemoveAt(i);
                    break;
                }
            }
        }
        if(result != null)
        {
            return result;
        }
        else
        {
            //No cards returned..
            Debug.Log("No valid cards were found", this);
            return null;
        }
    }

    //Private methods
}