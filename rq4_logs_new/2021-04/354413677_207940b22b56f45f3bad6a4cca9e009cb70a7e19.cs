using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Player : MonoBehaviour
{
    public HealthBar healthBar;
    public Transform spawnPoint;
    public int maxHealth = 100;
    int currentHealth;

    void Start()
    {
        currentHealth = maxHealth;
        
        if(healthBar != null)
            healthBar.SetMaxHealth(maxHealth);
    }

    public void TakeDamage(int damage)
    {
        //Animação de levar dano
        currentHealth -= damage;

        if(healthBar != null)
            healthBar.SetHealth(currentHealth);

        if(currentHealth <= 0)
        {
            Die();
            Debug.Log("Jogador morreu");
        }
    }

    void Die()
    {
        //Animação de morte
        //Destroy(gameObject);
        //respawn
    }
}