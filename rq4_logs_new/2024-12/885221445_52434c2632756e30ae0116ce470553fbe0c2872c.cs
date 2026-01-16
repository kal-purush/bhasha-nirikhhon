using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GameUI : MonoBehaviour
{
    public static GameUI Instance { set; get; }

    private void Awake()
    {
        Instance = this;
    }

    // Buttons
    public void OnLocalGameButton()
    {
        Debug.Log("OnLocalGameButton");
    }
    public void OnOnlineGameButton()
    {
        Debug.Log("OnOnlineGameButton");
    }

    public void OnOnlineHostButton()
    {
        Debug.Log("OnOnlineHostButton");
    }
    public void OnOnlineConnectButton()
    {
        Debug.Log("OnOnlineConnectButton");
    }
    public void OnOnlineBackButton()
    {
        Debug.Log("OnOnlineBackButton");
    }
}