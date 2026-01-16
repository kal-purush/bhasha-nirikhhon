using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnemyLife : MonoBehaviour
{
    public float health = 50f;

    DamageCalc damageCalc;
    public GameObject res;

    public void awake()
    {
        damageCalc = res.GetComponent<DamageCalc>();

    }

    public void update()
    {
        //damageCalc.daño;
    }

    public void TakeDamage()
    {
        //health -= daño;
        //int barhealth = daño * gameObject.transform.localScale * 100;
        if (health <= 0)
        {
            Die();
        }

    }

    void Die()
    {
        Destroy(gameObject);
    }
}