using UnityEngine;
using System.Collections;

public class Navigator : MonoBehaviour {

	public float moveSpeed = 10f;

	private Vector3 direction;

	void Update () {
		//Movement

		direction = new Vector3 (0, 0, 0);
		if ((Input.GetKey (KeyCode.W) || Input.GetKey (KeyCode.UpArrow)) && (Input.GetKey (KeyCode.D) || Input.GetKey (KeyCode.RightArrow)))
			direction = (Vector3.up + Vector3.right) / Mathf.Sqrt (2);
		else if ((Input.GetKey (KeyCode.W) || Input.GetKey (KeyCode.UpArrow)) && (Input.GetKey (KeyCode.A) || Input.GetKey (KeyCode.LeftArrow)))
			direction = (Vector3.up + Vector3.left) / Mathf.Sqrt (2);
		else if ((Input.GetKey (KeyCode.S) || Input.GetKey (KeyCode.DownArrow)) && (Input.GetKey (KeyCode.D) || Input.GetKey (KeyCode.RightArrow)))
			direction = (Vector3.down + Vector3.right) / Mathf.Sqrt (2);
		else if ((Input.GetKey (KeyCode.S) || Input.GetKey (KeyCode.DownArrow)) && (Input.GetKey (KeyCode.A) || Input.GetKey (KeyCode.LeftArrow)))
			direction = (Vector3.down + Vector3.left) / Mathf.Sqrt (2);
		else if(Input.GetKey(KeyCode.W) || Input.GetKey(KeyCode.UpArrow))
			direction = Vector3.up;

		else if(Input.GetKey(KeyCode.S) || Input.GetKey(KeyCode.DownArrow))
			direction = Vector3.down;
		
		else if(Input.GetKey(KeyCode.A) || Input.GetKey(KeyCode.LeftArrow))
			direction = Vector3.left;
		
		else if(Input.GetKey(KeyCode.D) || Input.GetKey(KeyCode.RightArrow))
			direction = Vector3.right;
		
		direction = direction.normalized;
	}

	void FixedUpdate ()
	{
		//Movement
		if (direction != new Vector3 (0, 0, 0))
			transform.Translate (moveSpeed * direction * Time.deltaTime);
	}

}