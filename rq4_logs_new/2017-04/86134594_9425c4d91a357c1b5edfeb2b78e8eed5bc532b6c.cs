using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BackCollision : MonoBehaviour {

	public GameObject cube;
	public float Speed;
	void OnTriggerStay(Collider col){
		if (col.tag == "Player") {
			cube.transform.position = new Vector3 (cube.transform.position.x, cube.transform.position.y, cube.transform.position.z + Time.deltaTime * Speed);
		}
		}
}