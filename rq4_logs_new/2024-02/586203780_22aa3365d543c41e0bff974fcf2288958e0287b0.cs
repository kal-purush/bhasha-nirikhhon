using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnemyStore : MonoBehaviour
{

    public static EnemyStore instance;

    public Enemy[] enemies;
    // Start is called before the first frame update
    void Start()
    {
        if (instance == null)
        {
            instance = this;
        }
    }

    public Enemy getRandomEnemy()
    {
        int randomIndex = UnityEngine.Random.Range(0, enemies.Length);

        return enemies[randomIndex];
    }

    public List<Enemy> getShuffledList(Enemy[] enemyArray)
    {
        System.Random random = new System.Random();
        List<Enemy> enemyList = new List<Enemy>(enemyArray);
        int n = enemyList.Count;
        while (n > 1)
        {
            n--;
            int k = random.Next(n + 1);
            Enemy value = enemyList[k];
            enemyList[k] = enemyList[n];
            enemyList[n] = value;
        }

        return enemyList;
    }

}