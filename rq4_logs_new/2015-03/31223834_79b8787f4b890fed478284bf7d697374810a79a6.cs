using UnityEngine;
using System.Collections;

public class Rocket : MonoBehaviour {
	// Speed of rocket
	public float speed = 2000.0f;
	
	// explosion prefab
	public GameObject explosionPrefab;
	
	// find out when it hits something
	void OnCollisionEnter(Collision c) {
		// show an explosion
		// transform.position because it should be where the rocket is
		// Quaternion.identity gives default rotation
		Instantiate(explosionPrefab,
		            transform.position,
		            Quaternion.identity);
		
		// destroy the rocket
		Destroy(gameObject);
	}
}