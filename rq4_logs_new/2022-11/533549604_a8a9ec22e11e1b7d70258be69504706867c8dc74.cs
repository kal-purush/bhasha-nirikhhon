using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AllySpawner : MonoBehaviour
{
    [SerializeField] private GameObject blocker, sniper;
    private float spawnTimer = 0;

    private void Update()
    {
        spawnTimer -= Time.deltaTime;
    }
    public void SpawnBlocker()
    {
        if (spawnTimer <= 0)
        {
            Instantiate(blocker, new Vector2(-5,-3), Quaternion.identity);
            spawnTimer = 3f;
        }
    }

    public void SpawnSniper()
    {
        if (spawnTimer <= 0)
        {
            Instantiate(sniper, new Vector2(-5,-3), Quaternion.identity);
            spawnTimer = 3f;
        }
    }
}