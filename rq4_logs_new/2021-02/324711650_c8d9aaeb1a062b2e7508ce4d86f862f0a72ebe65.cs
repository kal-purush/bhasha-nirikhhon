using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Security.Cryptography.X509Certificates;
using UnityEngine;
using UnityEngine.UI;

public class RoomData : MonoBehaviour
{
    public string roomName = "";
    public int playerCount = 0;
    public int maxPlayer = 0;

    [System.NonSerialized]
    public Text roomDataText;

    void Awake()
    {
        roomDataText = GetComponentInChildren<Text>();
    }

    public void UpdateInfo()
    {
        roomDataText.text = string.Format(" {0} [{1} / {2}]", roomName, playerCount.ToString("00"), maxPlayer);
    }
}