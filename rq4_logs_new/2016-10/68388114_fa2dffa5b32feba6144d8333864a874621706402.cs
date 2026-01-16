using UnityEngine;
using System.Collections;
using System;
using UnityEngine.UI;

public class UpdateInfo : MonoBehaviour {

    public static Text text;
    public static bool game_end = false;
    // Use this for initialization
    void Start () {
        text = GetComponent<Text>();
	}
	
	// Update is called once per frame
	void Update () {
        if (game_end != true)
        {
            UpdateText();
        }
    }

    private static void UpdateText()
    {
        text.text = "Coins: " + CoinCounter.GetCoin();
    }
    public static void UpdateText(string endgame)
    {
        text.fontSize = 15;
        text.color = Color.red;
        text.text = endgame;
    }
}