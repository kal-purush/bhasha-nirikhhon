using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DrawButtonController : MonoBehaviour
{
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
        if (state == GameManager.GameState.Refill)
        {
            this.gameObject.SetActive(false);
        }
        else
        {
            this.gameObject.SetActive(true);
        }
    }
    
    
}