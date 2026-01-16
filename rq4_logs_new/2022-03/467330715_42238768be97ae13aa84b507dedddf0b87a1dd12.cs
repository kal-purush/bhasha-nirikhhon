using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DealerController : MonoBehaviour
{
    
    private List<GameObject> m_Cards = new List<GameObject>();
    private void Awake()
    {
        GameManager.OnGameStateChange += StateChange;
    }

    private void OnDestroy()
    {
        GameManager.OnGameStateChange -= StateChange;
    }

    private void StateChange(GameManager.GameState state)
    {
        switch (state)
        {
            case GameManager.GameState.Draw:
                HandleDraw();
                break;
            case GameManager.GameState.Refill:
                HandleRefill();
                break;
            default:
                break;
        }
    }
    
    private void HandleDraw()
    {
        for (int i = 0; i < GameManager.HandTotal; i++)
        {
            m_Cards.Add(GameManager.Instance.GetCardFromDeck());
            m_Cards[m_Cards.Count-1].transform.SetParent(this.transform);
            Vector3 newPosition = m_Cards[m_Cards.Count - 1].transform.position;
            newPosition.z -= 1;
            m_Cards[m_Cards.Count - 1].transform.position = newPosition;
        }
    }

    private void HandleRefill()
    {
        for (int i = 0; i < GameManager.HandTotal; i++)
        {
            if (m_Cards[i] == null)
            {
                m_Cards[i] = GameManager.Instance.GetCardFromDeck();
                m_Cards[i].transform.SetParent(this.transform);
                Vector3 newPosition = m_Cards[m_Cards.Count - 1].transform.position;
                newPosition.z -= 1;
                m_Cards[m_Cards.Count - 1].transform.position = newPosition;
            }
        }
    }
    
    
    public List<CardData> GetCardData()
    {
        List<CardData> returnCards = new List<CardData>();

        for (int i = 0; i < m_Cards.Count; i++)
        {
            returnCards.Add(m_Cards[i].GetComponent<CardController>().GetCardData());
        }

        return returnCards;
    }
}