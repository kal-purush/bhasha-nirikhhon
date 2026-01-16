using UnityEngine;
using System.Collections;

public class ForcePusher : MonoBehaviour {
	//private GameObject redCube;
	private GameObject ground;
	private Rigidbody rb;
	private bool groundtouch;

	// Use this for initialization
	void Start () {
		//redCube = GameObject.Find ("redCube");
		ground = GameObject.Find ("ground");
		rb = gameObject.GetComponent<Rigidbody> ();
		groundtouch = false;
	}
	
	// Update is called once per frame
	void Update () {
	// when i hit the space key, move the object
		//ANYTHING THAT IS UPPERCASE GREEN CAN BE A VARIABLE!
		if (Input.GetKey(KeyCode.Space) && groundtouch == true){
			rb.AddForce (new Vector3 (0, 400, 0));
			rb.AddTorque (new Vector3 (0, 0, 0));
			//redCube.transform.localScale = new Vector3 (2,3,1);
			groundtouch = false;
		}

		if (Input.GetKey(KeyCode.A)){
			rb.AddForce (new Vector3 (-10, 0, 0));
		}
		if (Input.GetKey (KeyCode.D)){
			rb.AddForce (new Vector3 (10, 0, 0));
		}
	}

	void OnCollisionEnter( Collision collisionInfo ){
		if (collisionInfo.gameObject.name == ground.name) {
			groundtouch = true;
		}
	}
}