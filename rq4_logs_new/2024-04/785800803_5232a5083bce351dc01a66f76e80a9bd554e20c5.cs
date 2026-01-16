using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerController : MonoBehaviour
{
	// Variables
	public float speed = 5.0f; // maxSpeed
	public float acceleration = 2f; // acceleration
	public float jumpSpeed = 10.0f; // jumpForce multiplier
	public float doubleJumpSpeed = 10.0f; // doubleJumpForce multiplier
	public float raycastDistance = 0.1f; // raycast distance - for checking if the player is grounded
	public float gravity = 3.0f; // gravity
	// Private variables
	private bool hasDoubleJumped = false;
	private Rigidbody2D rb;
	private BoxCollider2D bc;
	// Start function
	void Start()
	{
		// Get the Rigidbody2D component
		rb = GetComponent<Rigidbody2D>();
		// Set the gravity scale of the rigidbody
		rb.gravityScale = gravity;
		// Get the BoxCollider2D component
		bc = GetComponent<BoxCollider2D>();
	}
	// Update function
	void Update()
	{
		// Add force to the rigidbody based on the player's input
		rb.AddForce(Vector2.right * Input.GetAxis("Horizontal") * speed * acceleration);
		// Limit the velocity of the rigidbody to the speed variable
		if (rb.velocity.x > speed)
		{
			// If the velocity is greater than the speed, set the velocity to the speed
			rb.velocity = new Vector2(speed, rb.velocity.y);
		}
		// Limit the velocity of the rigidbody to the negative speed variable
		else if (rb.velocity.x < -speed)
		{
			// If the velocity is less than the negative speed, set the velocity to the negative speed
			rb.velocity = new Vector2(-speed, rb.velocity.y);
		}
		// Check if the player is grounded
		if (Input.GetKeyDown(KeyCode.W) || Input.GetKeyDown(KeyCode.UpArrow))
		{
			// If the player is grounded, jump, if not then DoubleJump
			Jump();
		}
	}
	// Jump function
	void Jump()
	{
		// Check if the player is grounded
		if (IsGrounded())
		{
			// If the player is grounded, add force to the rigidbody
			rb.AddForce(Vector2.up * jumpSpeed, ForceMode2D.Impulse);
			// Set hasDoubleJumped to false
			hasDoubleJumped = false;
		}
		// If the player is not grounded and has not double jumped
		else if (!hasDoubleJumped)
		{
			// Add force to the rigidbody
			rb.AddForce(Vector2.up * doubleJumpSpeed, ForceMode2D.Impulse);
			// Set hasDoubleJumped to true
			hasDoubleJumped = true;
		}
	}
	// Check if the player is grounded
	bool IsGrounded()
	{
		// Shoot a raycast down from the player
		RaycastHit2D hit = Physics2D.Raycast(transform.position + Vector3.down, Vector3.down, raycastDistance);
		// If the raycast hits anything
		if (hit.collider != null)
		{
			// If the raycast hits anything return true
			return true;
		}
		// If the raycast does not hit anything, return false
		return false;
	}
}