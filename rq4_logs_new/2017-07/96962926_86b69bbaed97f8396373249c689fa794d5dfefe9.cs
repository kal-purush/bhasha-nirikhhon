using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerController : MonoBehaviour {
	public float speed = 10f;
	public float turnSpeed = 5f;

	private Rigidbody2D rBody;

	void Start() {
		rBody = this.GetComponent<Rigidbody2D>();
	}

	// http://gamecodeschool.com/unity/building-asteroids-arcade-game-in-unity/
	void Update() {
		float moveHorizontal = Input.GetAxisRaw("Horizontal");
		float moveVertical = Input.GetAxis("Vertical");

		rBody.transform.Rotate(0, 0, -moveHorizontal * turnSpeed);

		rBody.AddForce(rBody.transform.up * speed * moveVertical);
	}
}