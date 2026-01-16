using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using TMPro;
public class DispHealth : MonoBehaviour
{
    public TextMeshProUGUI ValueText;
    GameObject player;
    Health playerHealth;

    void Start()
    {
        player = GameObject.FindWithTag("Player");
        playerHealth = player.GetComponent<Health>();
    }
    void FixedUpdate() 
    {
        int currentHealth = playerHealth.GetHealth();
        ValueText.text = currentHealth.ToString();
    }
}