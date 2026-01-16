using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MeleeWeapon : MonoBehaviour
{
    public List<GameObject> enemyHit = new List<GameObject>(); 

    public void SetAttackActive(int currentCombo)
    {
        transform.GetChild(currentCombo).gameObject.SetActive(true);
    }

    public void SetAttackInactive(int currentCombo)
    {
        transform.GetChild(currentCombo).gameObject.SetActive(false);
    }

    public void RegisterHit(GameObject enemy)
    {
        if (!enemyHit.Contains(enemy))
        {
            enemyHit.Add(enemy);
        }
    }

    public void DamageEnemy()
    {

        foreach (GameObject enemy in enemyHit)
        {
            Debug.Log("Damage Enemy Method Started");
            enemy.GetComponent<EnemyStats>().TakeDamage(35f); //placeholder damage, will make damage base on the current attack anim
        }
    }

    public void ClearEnemyList()
    {
        enemyHit.Clear();
    }

}