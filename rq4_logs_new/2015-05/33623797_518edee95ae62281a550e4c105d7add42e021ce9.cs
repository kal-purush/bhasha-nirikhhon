using UnityEngine;
using System.Collections;

public class PlayerController : MonoBehaviour {
	
	//public GameManager 	gameManager;
	public float 		speed;
	
	private Animator 		anim;
	private Rigidbody2D 	body;
	private enum Direction { 
		Up = 0, 
		Down, 
		Left, 
		Right, 
		Unknown 
	};
	
	// Use this for initialization
	void Start () {
		
		anim = this.GetComponent<Animator> ();
		body = this.GetComponent<Rigidbody2D> ();
	}
	
	// Update is called once per frame
	void FixedUpdate () {
		
		float v = Input.GetAxis ("Vertical"); 
		float h = Input.GetAxis ("Horizontal"); 
		ManageMovement (h, v); 
		body.velocity = new Vector3 (h * speed, v * speed, 0);
	}
	
	void ManageMovement (float horizontal, float vertical) {
		
		if (horizontal != 0f || vertical != 0f) {
			anim.SetBool("moving", true);
			Walk (horizontal, vertical);
		} else {
			anim.SetBool("moving", false);
		}
	}
	
	void Walk (float h, float v) {
		
		if (!anim)
			return;
		
		if (v > 0 && v > h)
			anim.SetInteger ("direction", (int)Direction.Up);
		if (v < 0 && v < h)
			anim.SetInteger ("direction", (int)Direction.Down);
		if (h < 0 && h < v)
			anim.SetInteger ("direction", (int)Direction.Left);
		if (h > 0 && h > v)
			anim.SetInteger ("direction", (int)Direction.Right);
	}
	
	void OnCollisionEnter2D(Collision2D collision)
	{
		print ("anim collided with something");
		//gameManager.BroadcastMessage ("SetBattleState");
	}
}