using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class VehicleSpawn : MonoBehaviour {

   public GameObject vehicleGO;

	// Use this for initialization
	void Start () {
        StartCoroutine(SpawnCars());
	}
	
	// Update is called once per frame
	void Update () {
		
	}

    public IEnumerator SpawnCars(){
        yield return new WaitForSeconds(Random.Range(2.0f, 5.0f));
        var clone = Instantiate(vehicleGO, transform.position, transform.rotation, transform);
        Destroy(clone, 600f);
        StartCoroutine(SpawnCars());

    }
}