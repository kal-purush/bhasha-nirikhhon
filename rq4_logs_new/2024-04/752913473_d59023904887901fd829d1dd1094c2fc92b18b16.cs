using System.Collections;
using System.Collections.Generic;
using Unity.VisualScripting;
using UnityEngine;

public class MeleeDamage : MonoBehaviour
{
    List<GameObject> enemiesInRange;
    List<GameObject> npcsInRange;

    void Start()
    {
        enemiesInRange = new();
        npcsInRange = new();
    }

    void OnTriggerEnter(Collider other)
    {
        if (other.CompareTag("Enemy"))
            enemiesInRange.Add(other.gameObject);
        else if (other.CompareTag("NPC"))
            npcsInRange.Add(other.gameObject);
    }

    void OnTriggerExit(Collider other)
    {
        if (other.CompareTag("Enemy"))
            enemiesInRange.Remove(other.gameObject);
        else if (other.CompareTag("NPC"))
            npcsInRange.Remove(other.gameObject);
    }

    void RemoveDeadEnemies()
    {
        List<GameObject> remainingEnemies = new();

        foreach (GameObject enemy in enemiesInRange)
        {
            if (!enemy.IsDestroyed() && enemy.GetComponent<EnemyBehavior>().GetHealth() > 0)
            {
                remainingEnemies.Add(enemy);
            }
        }
        
        enemiesInRange = remainingEnemies;
    }

    public bool TryInteract()
    {
        if (npcsInRange.Count == 0) return false;

        foreach (GameObject npc in npcsInRange)
        {
            npc.GetComponentInChildren<NPCText>().CycleText();
        }

        return true;
    }

    public bool Attack(int damage)
    {
        RemoveDeadEnemies();

        if (enemiesInRange.Count == 0) return false;

        foreach (GameObject enemy in enemiesInRange)
        {
            enemy.GetComponent<EnemyBehavior>().TakeDamage(damage);
        }

        return true;
    }
}