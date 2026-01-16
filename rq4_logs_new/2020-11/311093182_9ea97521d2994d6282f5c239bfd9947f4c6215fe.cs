using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PropSpawner : MonoBehaviour
{
    // Show visual indicator of prop drop locator

    public float propMinSpawnDelay;
    public float propMaxSpanDelay;

    private BoxCollider spawnPlaneCol;
    private float timer;
    private float nextPropDelay;
    
    void Start()
    {
        spawnPlaneCol = GetComponent<BoxCollider>();
        timer = 0;
        nextPropDelay = newRndDelay();
    }
    
    void Update()
    {
        timer += Time.deltaTime;

        if (timer >= nextPropDelay)
        {
            //Debug.Log("Spawned new prop after " + timer + " seconds");
            timer = 0;
            nextPropDelay = newRndDelay();
            SpawnProp();
        }
    }

    float newRndDelay()
    {
        return Random.Range(propMinSpawnDelay, propMaxSpanDelay); ;
    }

    void SpawnProp()
    {
        GameObject newProp = Instantiate((GameObject)Resources.Load("props/ExampleProp", typeof(GameObject)));

        float spawnPlaneLPos = spawnPlaneCol.bounds.min.x;
        float spawnPlaneRPos = spawnPlaneCol.bounds.max.x;
        Vector3 newPropPos = new Vector3(Random.Range(spawnPlaneLPos, spawnPlaneRPos), transform.position.y);

        newProp.transform.position = newPropPos;
        newProp.transform.localScale = new Vector3(0.3f, 0.3f, 0.3f);
    }
}