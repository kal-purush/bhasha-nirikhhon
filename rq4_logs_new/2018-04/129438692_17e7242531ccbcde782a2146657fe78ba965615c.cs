using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnemyFlyGenerator : MonoBehaviour {

    public ObjectPooler enemyFlyPool;

    public float distanceBetweenEnemies;

    public void SpawnEnemies(Vector3 startPosition)
    {
        GameObject enemy1 = enemyFlyPool.GetPooledObject();
        enemy1.transform.position = startPosition;
        enemy1.SetActive(true);

        GameObject enemy2 = enemyFlyPool.GetPooledObject();
        enemy2.transform.position = startPosition;
        enemy2.SetActive(true);

        GameObject enemy3 = enemyFlyPool.GetPooledObject();
        enemy3.transform.position = startPosition;
        enemy3.SetActive(true);

    }
}