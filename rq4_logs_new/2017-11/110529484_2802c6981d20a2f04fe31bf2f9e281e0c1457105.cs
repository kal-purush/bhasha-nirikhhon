using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class bulletScript_start : MonoBehaviour {

	public float bulletSpeed=0.1f;

	// Use this for initialization
	void Start () {

		//Destroy the bullet after 8 seconds.
		Destroy (gameObject, 8);

	}
	
	// Update is called once per frame
	void Update () {
	
		transform.Translate (new Vector3(bulletSpeed, 0, 0));

	}

	void OnTriggerEnter(Collider other) {
		if (other.tag == "Enemy") {

			//1.Destroy the enemy and trigger the 'enemy explosion particle'.
			other.gameObject.GetComponent<enemyScript_start>().enemyDefeated();

			//2.This bullet is also destroyed
			bulletSpeed=0;
			gameObject.GetComponent<Rigidbody> ().Sleep ();

			Destroy(gameObject);

			}
		
		
		}


}