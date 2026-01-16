using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnemySpawner : MonoBehaviour
{
    [SerializeField] GameObject enemyPrefab;
    [SerializeField] float minSpawnTime = 2f;
    [SerializeField] float maxSpawnTime = 4f;

    bool spawning = false;

    void Update()
    {
        if (!spawning)
        {
            StartCoroutine(SpawnEnemy());
        }
    }

    IEnumerator SpawnEnemy()
    {
        spawning = true;
        yield return new WaitForSeconds(Random.Range(minSpawnTime, maxSpawnTime));
        Instantiate(enemyPrefab, transform.position, Quaternion.identity);
        spawning = false;
    }
}