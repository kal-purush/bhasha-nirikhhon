using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[RequireComponent(typeof(CharacterController))]
[AddComponentMenu("Control Script/FPS Input")]
public class FPSInput : MonoBehaviour {

	private CharacterController _charController;

	public float speed = 6.0f;
	public float gravity = -9.8f;
	// Use this for initialization
	void Start () {
		_charController = GetComponent<CharacterController> ();
		
	}
	
	// Update is called once per frame
	void Update () {
		//transform.Translate (speed, 0, 0);
		float deltaX = Input.GetAxis("Horizontal") * speed;
		float deltaZ = Input.GetAxis("Vertical") * speed;
		Vector3 movement = new Vector3 (deltaX, 0, deltaZ);
		movement = Vector3.ClampMagnitude (movement, speed);
		movement.y = gravity;

		movement *= Time.deltaTime;
		movement = transform.TransformDirection (movement);
		_charController.Move (movement);

		//transform.Translate (deltaX*Time.deltaTime, 0, deltaZ*Time.deltaTime);
	}
}