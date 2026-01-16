using UnityEngine;
using System.Collections;

public class playerBehaviour : MonoBehaviour {

	public float maxSpeed = 10f;

	Rigidbody2D rb;
	bool facingRight = true;
	float move;

	bool grounded = false;
	public Transform groundCheck;
	float groundRadius = 0.1f;
	public LayerMask whatIsGround;
	public float jumpForce = 700f;

	void Start () 
	{
		rb = GetComponent<Rigidbody2D>();
	}


	void FixedUpdate () 
	{
		grounded = Physics2D.OverlapCircle(groundCheck.position, groundRadius, whatIsGround);

		move = Input.GetAxis ("Horizontal");

		rb.velocity = new Vector2 (move * maxSpeed, rb.velocity.y);
	
		if (move > 0 && !facingRight)
			Flip ();
		else if (move < 0 && facingRight)
			Flip ();
	}

	void Update()
	{
		if (grounded && Input.GetButtonDown ("Jump")) 
		{
			grounded = false;
			rb.AddForce (new Vector2 (0, jumpForce));
		}
	}


	void Flip()
	{
		facingRight = !facingRight;
		Vector3 theScale = transform.localScale;
		theScale.x *= -1;
		transform.localScale = theScale;
	}
}