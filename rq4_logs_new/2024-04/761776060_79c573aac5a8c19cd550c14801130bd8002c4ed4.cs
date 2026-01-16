using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class FuelRode : MonoBehaviour
{

    [SerializeField] protected float damage;


    protected void OnTriggerStay2D(Collider2D other)
    {
        if(other.CompareTag("Player") && !PlayerController.Instance.pState.invincible)
        {
            PlayerController.Instance.TakeDamage(damage);
        }

        if(other.CompareTag("Enemy"))
        {
            other.GetComponent<Enemy>().EnemyHit(damage, (other.transform.position - transform.position).normalized, 0);
        }

        if(other.CompareTag("Boss"))
        {
            other.GetComponent<Enemy>().EnemyHit(damage, (other.transform.position - transform.position).normalized, 0);
        }
    }
}