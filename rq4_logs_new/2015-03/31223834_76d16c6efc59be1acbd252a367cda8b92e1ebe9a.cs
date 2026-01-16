using UnityEngine;
using System.Collections;

public class EndGameTrigger : Interactable {

private CardboardHead head;
public GameObject player;

void Start(){
	player = GameObject.FindWithTag("MainCamera"); // Find the object tagged as MainCamera
	head = Camera.main.GetComponent<StereoController>().Head;
}

void Update(){

		RaycastHit hit;
		bool isLookedAt = GetComponent<Collider> ().Raycast (head.Gaze, out hit, Mathf.Infinity);

		if (isLookedAt) {

			GetComponent<Renderer> ().material.color = Color.green;
			interacting = true;

			if (Cardboard.SDK.CardboardTriggered) {
				Application.LoadLevel ("GameOver");
			}
		}

		if (!isLookedAt) {
			GetComponent<Renderer> ().material.color = Color.white;
			interacting = false;
		}
	}

}