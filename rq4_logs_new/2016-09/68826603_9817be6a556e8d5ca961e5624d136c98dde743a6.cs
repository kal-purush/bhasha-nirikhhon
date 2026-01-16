using UnityEngine;
using System.Collections;

public class MainPlayer : MonoBehaviour {
	bool inAir = true;
	float
	rotateSpeed = 200f,
	walkSpeed = 10f,
	jumpHeight = 50f;

	// Use this for initialization
	void Start () {
	
	}
	
	void Move() {
		if (Input.GetKey("up")) {
			GetComponent<Rigidbody>().MovePosition(transform.position + transform.forward * Time.deltaTime * walkSpeed);
		}
		if (Input.GetKey("right")) {
			GetComponent<Rigidbody>().transform.Rotate(Vector3.up, rotateSpeed * Time.deltaTime);
		}
		if (Input.GetKey("left")) {
			GetComponent<Rigidbody>().transform.Rotate(Vector3.up, -rotateSpeed * Time.deltaTime);
		}
		if (Input.GetKey("down")) {
			GetComponent<Rigidbody>().MovePosition(transform.position - transform.forward * Time.deltaTime * (0.5f * walkSpeed));
		}
		if (Input.GetKey("space")) {
			if (!inAir) {
				inAir = true;
				GetComponent<Rigidbody>().AddForce(Vector3.up * jumpHeight, ForceMode.Impulse);
			}
		}
	}
	// Update is called once per frame
	void Update () {
	
	}

	void FixedUpdate() {
		Move();
	}
}