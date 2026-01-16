using UnityEngine;
using System.Collections;

public class Bullet : MonoBehaviour {

	public int bulletSpeed;
	public int travelLength;
	Vector3 spawnPoint;

	// Use this for initialization
	void Start () {

		spawnPoint = gameObject.transform.position;

	}

	// Update is called once per frame
	void FixedUpdate () {

		transform.Translate (Vector3.right * Time.deltaTime * bulletSpeed);

		if (gameObject.transform.position.x > travelLength + spawnPoint.x) {

			gameObject.transform.position = spawnPoint;

		}

	}
}