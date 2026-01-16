using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class SphereController : MonoBehaviour {
    private List<GameObject> memories;
    private SphereSpawner spawner;

	// Use this for initialization
	void Start () {
        spawner = gameObject.GetComponent<SphereSpawner>();
        memories = spawner.memories;
	}
	
	// Update is called once per frame
	void Update () {
        foreach (GameObject memory in memories) {
            Movement move = memory.GetComponent<Movement>();
            move.Move();
        }
    }
}