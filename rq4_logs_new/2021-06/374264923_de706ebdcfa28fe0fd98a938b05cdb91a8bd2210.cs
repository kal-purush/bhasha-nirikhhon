using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EndMarkerSpawning : MonoBehaviour
{
    bool nextSpawn = false;
    public GameObject spawner;

    public Spawner spawnScript;
    void Start() {
        spawner = GameObject.Find("Spawner");
        spawnScript = spawner.GetComponent<Spawner>();
    }
    void Update()
    {
        if(transform.position.z <= 11 && !nextSpawn) {
            spawnScript.SpawnObject();
            nextSpawn = true;
        }
    }
}