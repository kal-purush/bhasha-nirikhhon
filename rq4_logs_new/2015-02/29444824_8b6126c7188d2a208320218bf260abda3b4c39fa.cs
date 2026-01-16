using UnityEngine;
using System.Collections;

public class ButtonScript : MonoBehaviour {

	bool isActive;	//Whether or not the button can be pressed
	GameObject cam;
	SpikeControl controlScript;
	GameObject[] platforms;
	GameObject currPlatform;
	float diffY;

	// Use this for initialization
	void Start () {
		isActive = false;
		diffY = 0.28f;
		platforms = GameObject.FindGameObjectsWithTag ("Platform");
		cam = GameObject.FindGameObjectWithTag ("MainCamera");
		controlScript = (SpikeControl)cam.GetComponent (typeof(SpikeControl));
		foreach (GameObject platform in platforms) {
			if((platform.transform.position.y + 0.28f) == transform.position.y) {
				currPlatform = platform;
			}
		}
	}
	
	// Update is called once per frame
	void Update () {
		if (controlScript.getCanRespawn ()) {
			isActive = true;
		}

		if (isActive) {
			renderer.material.color = Color.green;
		} else {
			renderer.material.color = Color.red;
		}
	}

	void press() {
		controlScript.respawnSpikes ();
		isActive = false;
		reposition ();
	}

	void reposition() {
		bool foundPos = false;
		GameObject newPlatform = currPlatform;
		while (!foundPos) {
			newPlatform = platforms[Random.Range(0, platforms.Length)];
			if(newPlatform != currPlatform) {
				foundPos = true;
			}
		}
		Vector2 newPos = new Vector2 (newPlatform.transform.position.x, newPlatform.transform.position.y + diffY);
		transform.position = newPos;
		currPlatform = newPlatform;
	}

	void OnCollisionEnter2D(Collision2D coll) {
		if (coll.gameObject.tag == "Player") {
			if(isActive) {
				press();
			}
		}
	}
}