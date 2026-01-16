using UnityEngine;
using System.Collections;

public class Shrub : MonoBehaviour {


	private Vector3 spawnPoint;
	private Vector3 bottomPoint;
	public GameObject bottomArea;
	
	private float speed;
	private float lerpTime = 1f;
	private float curLerpTime;

	// Use this for initialization
	void Start () {
		spawnPoint = gameObject.transform.position;
		speed = .1f;
		curLerpTime = 0;
		bottomPoint = bottomArea.transform.position;
	}
	
	// Update is called once per frame
	void Update () {
		curLerpTime += Time.deltaTime * speed;
		if (curLerpTime > lerpTime) {
			curLerpTime = lerpTime;
			Destroy (gameObject);
		}
		
		float perc = curLerpTime / lerpTime;
		transform.position = Vector3.Lerp (spawnPoint, bottomPoint, perc);
	}
}