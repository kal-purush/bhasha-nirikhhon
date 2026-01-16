using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class TheHuhn : MonoBehaviour
{
    public GameObject[] enemies;

    public Transform[] spawnPoints;
    public int numberOfEnemies = 3;
    public float timeBetweenWaves = 2;
    public int delta = 2;



    private float nextWave;

    void Update()
    {
        if (nextWave < Time.time)
        {
            StartCoroutine(SpawnEnemies((int)(numberOfEnemies* delta)));
            nextWave = Time.time + timeBetweenWaves;
            numberOfEnemies += delta;
        }
    }

    IEnumerator SpawnEnemies(int number)
    {
        int rand;
        GameObject o;
        Transform t;
        for (int i = 0; i < number; i++)
        {
            rand = Random.Range(0, enemies.Length);
            o = enemies[rand];

            rand = Random.Range(0, spawnPoints.Length);
            t = spawnPoints[rand];

            Instantiate(o, t.position, Quaternion.identity, t);

            yield return 0.5f;
        }
    }
}