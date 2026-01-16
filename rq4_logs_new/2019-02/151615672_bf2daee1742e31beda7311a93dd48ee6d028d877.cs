using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class PlayerHealthText : MonoBehaviour {
    private Text textInfo;
    private PlayerHealth playerHealthInfo;
    private float playerHealth;
    private float playerHealthMax;
	// Use this for initialization
	void Start ()
    {
        textInfo = gameObject.GetComponent<Text>();
        playerHealthInfo = GameObject.Find("Player").GetComponent<PlayerHealth>();

    }

    private void FixedUpdate()
    {
        playerHealth = playerHealthInfo.playerHealth;
        playerHealthMax = playerHealthInfo.maxPlayerHealth;
        textInfo.text = "Player Health: " + playerHealth + "/" + playerHealthMax;

    }

  
}