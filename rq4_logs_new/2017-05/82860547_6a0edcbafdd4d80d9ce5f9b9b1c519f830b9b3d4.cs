using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class calculateMana : MonoBehaviour {
    float maxMana;
    float currentMana;
    Slider manaBar;
    Text manaText;
    
	// Use this for initialization
	void Start () {
        //maxMana = EndTurn.ownedPlayer.GetComponent<Player>().maxMana;
        //currentMana = EndTurn.ownedPlayer.GetComponent<Player>().mana;
        manaBar = this.GetComponent<Slider>();
        manaText = GetComponentInChildren<Text>();
	}
	
	// Update is called once per frame
	void Update () {
        if (EndTurn.ownedPlayer != null)
        {
            float value = EndTurn.ownedPlayer.GetComponent<Player>().mana / EndTurn.ownedPlayer.GetComponent<Player>().maxMana;
            manaBar.value = value;
            manaText.text = EndTurn.ownedPlayer.GetComponent<Player>().mana.ToString();
        }
    }
}