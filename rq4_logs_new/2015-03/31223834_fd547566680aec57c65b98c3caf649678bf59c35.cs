using UnityEngine;
using System.Collections;

public class Door : MonoBehaviour {

	private Animator animator;
	private bool doorOpen;

	//boilerplate
	void Start(){
		doorOpen = false;
		animator = GetComponent<Animator>();
	}

	//open door
	void OnTriggerEnter(Collider obj){

		if (obj.gameObject.name == "Head") {
			Debug.Log("Enter");
			doorOpen = true;
			DoorController("Open");
		}
	}

	//close door
	void OnTriggerExit(Collider obj){
		if (doorOpen) {
			Debug.Log("Exit");
			doorOpen = false;
			DoorController("Close");
		}

	}

	//Passes the state we want to the state diagram in the animator
	private void DoorController(string state){
		animator.SetTrigger(state);
	}
}