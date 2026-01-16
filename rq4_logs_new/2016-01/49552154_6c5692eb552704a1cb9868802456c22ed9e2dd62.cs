using UnityEngine;
using System.Collections;

public class TriggerDoor : MonoBehaviour {

	GameObject doorPressurePlate;
	pressureplates pp;

	// Use this for initialization
	void Start () {
		doorPressurePlate = GameObject.Find ("TriggerDoor");
		pp = doorPressurePlate.GetComponent<pressureplates> ();
	}

	// Update is called once per frame
	void Update () {
		if (pp.moveDoor) {
			Destroy(this.gameObject);
		}

	}
}