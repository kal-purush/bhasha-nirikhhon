using UnityEngine;
using System.Collections;

public class CharacterController : MonoBehaviour
{

	public float  speed;
	

	void FixedUpdate () 
	{

		float moveHorizontal = Input.GetAxisRaw ("Horizontal");
		float moveVertical = Input.GetAxisRaw("Vertical");

		Vector3 movement = new Vector3 (moveHorizontal, 0.0f, moveVertical);

		rigidbody.AddForce (movement * speed * Time.deltaTime);
	}
}