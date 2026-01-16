using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnemyHealth : MonoBehaviour
{
    [SerializeField] private float health = 1f;
    [SerializeField] GameObject parent;

    public void ChangeHealth(float amount)
    {
        health += amount;
        if (health <= 0)
        {
            Destroy(parent);
        }
    }

    public float GetHealth()
    {
        return health;
    }

    void Update()
    {
        if (health <= 0)
        {
            Destroy(parent);
        }
    }
}