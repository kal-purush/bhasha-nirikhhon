using System.Collections;
using System.Collections.Generic;
using System.Drawing.Printing;
using UnityEngine;

public class WaveSpawner : MonoBehaviour
{
    public Wave[] waves;
    private Wave currentwave;
    [SerializeField] private Transform[] SpawnPoints;

    private float timeBetweenSpawn;
    private int i = 0;

    private bool StopSpawning = false;

    private void Awake()
    {
        currentwave = waves[i];
        timeBetweenSpawn = currentwave.TimeBeforeThisWave;
    }

    private void Update()
    {
        if (StopSpawning)
        {
            return;
        } 

        if(Time.time >= timeBetweenSpawn)
        {
            SpawnWave();
            IncWave();

            timeBetweenSpawn = Time.time + currentwave.TimeBeforeThisWave;
        }
    }

    private void SpawnWave()
    {
        for(int i = 0; i < currentwave.NumberToSpawn; i++)
        {
            int num = Random.Range(0, currentwave.EnemiesInWaves.Length);   
            int num2 = Random.Range(0, SpawnPoints.Length);

            Instantiate(currentwave.EnemiesInWaves[num], SpawnPoints[num2].position, SpawnPoints[num2].rotation);
        }
    }

    private void IncWave()
    {
        if(i + 1 < waves.Length)
        {
            i++;
            currentwave = waves[i];
        }
        else
        {
            StopSpawning = true;
        }
    }
}