using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GameManagerScript : MonoBehaviour {

    // Objects //
    public Sprite[] cardImages = new Sprite[37];
    private DeckManager deckManager = new DeckManager();

    // Constants //
    private const int NULL_ID = -1000;

    // Variable //
    private int parent = NULL_ID;
    
    // Cards //
    private List<int>[] hands = new List<int>[4];
    private List<int>[] tables = new List<int>[4];

	// Use this for initialization
	void Start () {

        Random.SetSeed(DateTime.Now.Millisecond);

        for (int i = 0; i < 4; i++)
        {
            hands[i] = new List<int>();
            tables[i] = new List<int>();
        }

        Initialize_NewGame();
	}
	
	// Update is called once per frame
	void Update () {
		
	}

    private void Initialize_NewGame()
    {
        deckManager.Initialize_NewGame();

        Initialize_NextRound();
    }

    private void Initialize_NextRound()
    {
        deckManager.Initialize_NextRound();

        for (int i = 0; i < 4; i++)
        {
            hands[i].Clear();
            tables[i].Clear();
        }

        Deal();
    }

    
    private void Deal()
    {
        for (int p = 0; p < hands.Length; p++) {
            for (int i = 0; i < 13;i++)
            {
                hands[p].Add(deckManager.DrawCard());
            }
        }

        ShowHands(0);
    }

    private void ShowHands(int player)
    {
        for (int i = 0; i < hands[player].Count; i++)
        {
            GameObject card = new GameObject();
            card.AddComponent<SpriteRenderer>().sprite = cardImages[Rules.IdChangeSerialToCard(hands[player][i])];
            card.transform.localScale = new Vector2(1.5f, 1.5f);
            card.transform.position = new Vector2(-7+i, -3);
        }
    }

    private void ShowHands_All()
    {
        for(int i = 0; i < hands.Length; i++)
        {
            ShowHands(i);
        }
    }
}

public class DeckManager
{
    // Variable //
    private int deckUsedIndex;

    // Cards //
    private int[] deck = new int[136];

    public void Initialize_NewGame()
    {
        for (int i = 0; i < deck.Length; i++)
        {
            deck[i] = i;
        }
    }

    public void Initialize_NextRound()
    {
        deckUsedIndex = -1;
        Shuffle();
    }

    private void Shuffle()
    {
        for (int i = 0; i < deck.Length; i++)
        {
            int keep = deck[i];
            int r = Random.Xor128_next(deck.Length);
            deck[i] = deck[r];
            deck[r] = keep;
        }
    }

    public int DrawCard()
    {
        deckUsedIndex++;
        return deck[deckUsedIndex];
    }
}

