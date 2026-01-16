using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class SetPrintGold : MonoBehaviour {
    private GameManager GM;
    private string PrintGold = null;
    private Text UIText;

    void Start()
    {
        GM = GameManager.instance;
        UIText = GetComponent<Text>();
    }

    void GetPlayerCombo()
    {
        if (PrintGold != "Gold " + GM.Gold.ToString())
        {
            PrintGold = "Gold " + GM.Gold.ToString();
            UIText.text = PrintGold;
        }
    }

    void Update()
    {
        GetPlayerCombo();
    }
}