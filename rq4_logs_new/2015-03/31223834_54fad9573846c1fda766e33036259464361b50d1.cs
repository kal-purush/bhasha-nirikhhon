using UnityEngine;
using System.Collections;

public class Keypad : MonoBehaviour{

	private CardboardHead head;
	private GameObject player;
	public GameObject doorObj;
	private DoorScript doorScript;
	float distance = 10.0F;
	public bool keypadUnlocked;

	void Start(){

		player = GameObject.FindWithTag("MainCamera"); // Find the object tagged as MainCamera
		head = Camera.main.GetComponent<StereoController>().Head;
		keypadUnlocked = true;

	}

	void Update(){

		doorScript = doorObj.GetComponent<DoorScript> ();
		doorScript.isLocked ();

		distance = Vector3.Distance (this.transform.position, player.transform.position);

		RaycastHit hit;
		bool isLookedAt = GetComponent<Collider>().Raycast(head.Gaze, out hit, Mathf.Infinity);

		Debug.Log (keypadUnlocked);
		Debug.Log ("door:" + doorScript.isLocked ());
		Debug.Log (isLookedAt);

		if ((isLookedAt && distance < 2 && keypadUnlocked && doorScript.isLocked ())) {
			
			GetComponent<Renderer> ().material.color = Color.green;
			
			if (Cardboard.SDK.CardboardTriggered) {
				doorScript.setLocked(true);
			}
		}

		else if (keypadUnlocked == false){

			GetComponent<Renderer> ().material.color = Color.red;

		}

		if (!isLookedAt || distance < 2 || !doorScript.isLocked()){
			GetComponent<Renderer> ().material.color = Color.white;
		}

	}
}