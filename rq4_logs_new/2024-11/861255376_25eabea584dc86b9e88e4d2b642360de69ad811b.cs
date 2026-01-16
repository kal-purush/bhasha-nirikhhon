using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class HealthBar : MonoBehaviour
{
    PlayerHealth playerHealth;
    List<SpriteRenderer> healthBarsSprite = new List<SpriteRenderer>();

    private void Awake()
    {
        playerHealth = transform.parent.parent.Find("Status").GetComponent<PlayerHealth>();
        for (int i = 0; i < playerHealth.maxHealth; i++)
            healthBarsSprite.Add(transform.GetChild(i).GetComponent<SpriteRenderer>());
    }

    private void Update()
    {
        for(int i = 0; i < playerHealth.currentHealth; i++)
        {
            healthBarsSprite[i].color = new Color(255, 0, 0);
        }
        for (int i = playerHealth.currentHealth; i < playerHealth.maxHealth; i++)
        {
            healthBarsSprite[i].color = new Color(225, 225, 225);
        }
    }
}