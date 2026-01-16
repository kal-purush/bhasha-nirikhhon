using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class randomRotation : MonoBehaviour {

	// Use this for initialization
	void Start () {
        transform.rotation = Quaternion.Euler(Random.Range(0, 90), Random.Range(0, 90), Random.Range(0, 90));
        GetComponent<Rigidbody>().angularVelocity =  new Vector3(Random.Range(0, 4), Random.Range(0, 4), Random.Range(0, 4));
	}

}