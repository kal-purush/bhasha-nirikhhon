using System;
using UnityEngine;
using Random = UnityEngine.Random;

public abstract class BaseItemSpawner : MonoBehaviour, IItemSpawner {
    
    public ItemSpawnerVariables vars;

    protected float timeBetSpawn;
    protected float lastSpawnTime;

    protected void Start() {
        timeBetSpawn =
            Random.Range(vars.timeBetSpawnMin, vars.timeBetSpawnMax);
        lastSpawnTime = 0;
    }
    
    private void Update() {
        if (Time.time >= lastSpawnTime + timeBetSpawn) {
            lastSpawnTime = Time.time;
            timeBetSpawn = Random.Range(vars.timeBetSpawnMin,
                vars.timeBetSpawnMax);
            Spawn();
        }
    }

    public abstract void Spawn();

}