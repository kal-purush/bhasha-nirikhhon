using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ControllerClimbing : MonoBehaviour {
	
	private Player thePlayer;

	void Start(){
		thePlayer = FindObjectOfType<Player> ();
	}
		
	void OnTriggerEnter2D(Collider2D coll){
		if (coll.gameObject.tag == "ClimbingWall") {
			thePlayer.onWall = true;
		}
	}
	void OnTriggerExit2D(Collider2D coll){
		if(coll.gameObject.tag == "ClimbingWall"){
			thePlayer.onWall = false;
		}

	}


		
}