using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DeckLists : MonoBehaviour {
    public GameObject deck_panel;
    public List<GameObject> deck_list = new List<GameObject>();

    void Start()
    {
        deck_list = GlobalControl.Instance.deck_list;
    }

    public void SaveDeck()
    {
        GlobalControl.Instance.deck_list = deck_list;
    }

    public void returnToMenu()
    {
        Card[] deck = deck_panel.GetComponentsInChildren<Card>();
        if (deck.Length == 20)
        {
            GlobalControl.Instance.deck_list.Clear();
            foreach (Card card in deck)
            {
                deck_list.Add(card.gameObject);
            }
        }
        SaveDeck();
    }
}