using System.Collections.Generic;
using UnityEngine;
using System;
using UnityEngine.Assertions;
using UnityEngine.SceneManagement;
using Random = UnityEngine.Random;

public class GameManager : MonoBehaviour
{
    public enum GameState
    {
        Start,
        Play,
        Draw,
        Compare,
        End,
    }

    public static event Action<GameState> OnGameStateChange;

    public static GameManager Instance;

    public GameState State;

    [SerializeField] private GameObject CardPrefab;

    public int ShoeSize;

    private List<CardDeck> m_Decks = new List<CardDeck>();
    
    
    public void Awake()
    {
        Instance = this;
        Assert.IsNotNull(CardPrefab);
        if (ShoeSize == 0)
        {
            ShoeSize = 4;
        }
        GameManager.OnGameStateChange += StateChange;
        SceneManager.LoadScene("Scenes/Poker/PokerUI", LoadSceneMode.Additive);

    }

    public void Start()
    {
        Instance.UpdateGameState(GameState.Start);
        for (int i = 0; i < ShoeSize; i++)
        {
            m_Decks.Add(new CardDeck());
            m_Decks[i].FillDeck();
        }
    }
    
    private void StateChange(GameState state)
    {
        switch (state)
        {

        }
    }
    
    public void UpdateGameState(GameState state)
    {
        State = state;
        OnGameStateChange?.Invoke(state);
    }


    public GameObject GetCardFromDeck()
    {
        int randomDeck = Random.Range(0, ShoeSize - 1);
        GameObject card = Instantiate(CardPrefab);
        card.GetComponent<CardController>().SetCard(m_Decks[randomDeck].GetRandomCardData());
        return card;
    }
    
}