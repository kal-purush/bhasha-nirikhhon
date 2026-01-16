using System.Collections;
using UnityEngine;

public class ActivateBlock : MonoBehaviour {

	public float speed = 0.1f;
	private bool platupdate = false;
	public GameObject platform;


	void Update(){
		if (platupdate && platform.transform.position.y > 5) {

			platform.transform.Translate (new Vector3 (0, -speed, 0));
		} 
		else if (!platupdate && platform.transform.position.y < 15){
			platform.transform.Translate (new Vector3 (0, speed, 0));
		}

	}

	void OnCollisionStay (Collision collision){
		platupdate = true;
	}
	void OnCollisionExit (Collision collision){
		platupdate = false;
	}
}