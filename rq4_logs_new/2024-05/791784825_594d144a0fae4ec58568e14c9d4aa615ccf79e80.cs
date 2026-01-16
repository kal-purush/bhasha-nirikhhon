using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnemyAttack : MonoBehaviour
{
    public float enemyAttackDamage;
    public List<GameObject> targetHit = new List<GameObject>();

    private void OnTriggerEnter(Collider other)
    {
        if (other.gameObject.CompareTag("Player")) //add more tag if you need to make the enemy attack interact with more than just the player
        {
            targetHit.Add(other.gameObject);
        }
    }

    private void OnTriggerExit(Collider other)
    {
        if (other.gameObject.CompareTag("Player")) 
        {
            targetHit.Remove(other.gameObject);
        }
    }

    public void SetAttackActive()
    {
        transform.gameObject.SetActive(true);
    }

    public void SetAttackInactive()
    {
        transform.gameObject.SetActive(false);
    }

    public void DamageTarget()
    {
        foreach (GameObject target in targetHit)
        {
            if (target.CompareTag("Player"))
            {
                target.GetComponent<PlayerStats>().TakeDamage(enemyAttackDamage);
            }
        }
    }

    public void ClearTargetFromList()
    {
        targetHit.Clear();
    }


}