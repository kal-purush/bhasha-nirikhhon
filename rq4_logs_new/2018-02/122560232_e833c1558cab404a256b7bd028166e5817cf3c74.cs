using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerJump : MonoBehaviour {

	private static Rigidbody playerRB;
	public static int jumpHeight = 350;
	private bool isFalling = false;

	// Use this for initialization
	void Start () {
		playerRB = GetComponent<Rigidbody> ();
	}
	
	// Update is called once per frame
	void Update () {



		if (playerRB.velocity.y != 0) {
			isFalling = true;
		}

		if(Input.GetKeyDown(KeyCode.Space) && !isFalling){

			Vector3 jumpForce = new Vector3 (0, jumpHeight, 0);
			playerRB.AddForce (jumpForce);
			isFalling = true;


		}
	}

	void OnCollisionStay(Collision collisionInfo) {
		isFalling = false;
	}
}