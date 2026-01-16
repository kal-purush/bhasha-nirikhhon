using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class RainSpawning : MonoBehaviour
{
    public GameObject [] rainFormat;
    bool stopSpawning = false;
    float spawnTime = 1f;
    float spawnDelay = 0.1f;
    void Start()
    {
        InvokeRepeating("SpawnObject", spawnTime, spawnDelay);        
    }

    public void SpawnObject()
    {
        System.Random random = new System.Random();
        int num = random.Next(0, 2);
        Instantiate(rainFormat[num], transform.position, transform.rotation);
        if (stopSpawning)
        {
            CancelInvoke("SpawnObject");
        }
    }
}