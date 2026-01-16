using UnityEngine;
using System.Collections;

public class GroundCheck : MonoBehaviour {

	private PlayerMovement pm;

	// Use this for initialization
	void Start () {
		pm = gameObject.GetComponentInParent<PlayerMovement> ();
	}
	
	void OnTriggerEnter(Collider other){
		pm.onGround = true;
	}

	void OnTriggerExit(Collider other){
		pm.onGround = false;
	}
}