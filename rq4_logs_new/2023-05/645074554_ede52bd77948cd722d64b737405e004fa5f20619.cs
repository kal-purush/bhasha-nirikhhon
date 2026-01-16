using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Health : MonoBehaviour
{
    [SerializeField] int maxHealth = 100;
    int health;

    void Awake()
    {
        health = maxHealth;
    }

    public int GetHealth()
    {
        return health;
    }

    public void TakeDamage(int damage)
    {
        health -= damage;
        CheckHealthStatus();
    }

    void CheckHealthStatus()
    {
        if (health <= 0)
        {
            Destroy(gameObject);
        }
    }
}