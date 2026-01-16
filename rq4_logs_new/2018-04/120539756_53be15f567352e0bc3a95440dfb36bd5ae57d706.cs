using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class NPCSpawnManager : MonoBehaviour {

    public GameObject npcObj;
    public GameObject playerObj;
    public GameObject geesnado;
    public int numToSpawn = 10;
    
	// Use this for initialization
	void Start () {
		for(int i=0; i< numToSpawn; i++)
        {
            var spawnBox = geesnado.transform.localScale;
            var position = new Vector3(Random.Range(0,75) * spawnBox.x, Random.Range(0, 75) * spawnBox.x, 0);
            position = transform.TransformPoint(position - spawnBox /2 );
            position.z = 0;
            var obj = Instantiate(npcObj, position, transform.rotation);
        }
	}
	
	// Update is called once per frame
	void Update () {
		
	}
}