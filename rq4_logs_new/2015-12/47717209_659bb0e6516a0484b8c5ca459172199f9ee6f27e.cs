using UnityEngine;
using System.Collections;

public class CarSpawner : MonoBehaviour {

    public enum SpawnArea { North, East, South, West }

    public SpawnArea currentDirection = SpawnArea.South;

    public GameObject car;

    double spawnTimer = 3;

    // Use this for initialization
    void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
        if (spawnTimer < 0)
        {
            GameObject.Instantiate(car, transform.position, Quaternion.identity);
            spawnTimer = 3;
        }
        else
        {
            spawnTimer -= Time.deltaTime;
        }
	}
}