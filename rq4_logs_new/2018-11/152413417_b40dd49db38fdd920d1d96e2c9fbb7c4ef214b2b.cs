using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnemySpawner : MonoBehaviour
{
    public GameObject[] enemies;

    Vector2 spawnPoint;

    [Space]

    public float spawnRate;
    float rand;
    float nextSpawn;
    int difficulty;

    public float randRangeMax = 4;
    public float randRangeMin = -4;

    private void Update()
    {
        if (Time.time > nextSpawn)
        {
            //ändrar tiden det tar att spawna nya fiender.
            nextSpawn = Time.time + spawnRate;
            //Tar fram ett värde på y-axeln som fienden kommer spawna på
            rand = Random.Range(randRangeMax, randRangeMin);
            spawnPoint = new Vector2(transform.position.x, rand);

            //Väljer en av alla enemies jag lagt in
            Instantiate(enemies[Random.Range(0, enemies.Length)], spawnPoint, Quaternion.identity);
        }

        if (GameController.score >= difficulty && difficulty <= 5000)
        {
            spawnRate -= 0.12f;
            difficulty += 500;

        }
    }
}