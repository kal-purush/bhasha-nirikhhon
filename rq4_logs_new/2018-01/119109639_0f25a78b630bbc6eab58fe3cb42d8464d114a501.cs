using System;
using UnityEngine;
using UnityEngine.SceneManagement;

namespace UnityStandardAssets._2D
{
	public class enemyCharacter2D : MonoBehaviour
	{
		[SerializeField] private float m_MaxSpeed = 10f;                    // The fastest the player can travel in the x axis.
		//[SerializeField] private float m_JumpForce;                  // Amount of force added when the player jumps.
		[SerializeField] private float m_GravityScaleWithHeight = 0.1f;                  // Amount of force added when the player jumps.
		//[Range(0, 1)] [SerializeField] private float m_CrouchSpeed = .36f;  // Amount of maxSpeed applied to crouching movement. 1 = 100%
		[SerializeField] private bool m_AirControl = true;                 // Whether or not a player can steer while jumping;
		[SerializeField] private LayerMask m_WhatIsGround;                  // A mask determining what is ground to the character

		private Transform m_GroundCheck;    // A position marking where to check if the player is grounded.
		const float k_GroundedRadius = .2f; // Radius of the overlap circle to determine if grounded
		public bool m_Grounded;            // Whether or not the player is grounded.
		private Transform m_CeilingCheck;   // A position marking where to check for ceilings
		const float k_CeilingRadius = .01f; // Radius of the overlap circle to determine if the player can stand up
		private Animator m_Anim;            // Reference to the player's animator component.
		private Rigidbody2D m_Rigidbody2D;
		//private bool m_FacingRight = true;  // For determining which way the player is currently facing.
		private float timer;
		public int lootTime;
		private bool looting;
		public Animator m_houseAnim;

		private void Awake()
		{
			// Setting up references.
			m_GroundCheck = transform.Find("GroundCheck");
			m_CeilingCheck = transform.Find("CeilingCheck");
			m_Anim = GetComponent<Animator>();
			m_Rigidbody2D = GetComponent<Rigidbody2D>();
		}


		private void FixedUpdate()
		{
			m_Grounded = false;
			//m_Rigidbody2D.velocity = new Vector2(0, 0);
			m_Rigidbody2D.gravityScale = 0;
			// The player is grounded if a circlecast to the groundcheck position hits anything designated as ground
			// This can be done using layers instead but Sample Assets will not overwrite your project settings.
			Collider2D[] colliders = Physics2D.OverlapCircleAll(m_GroundCheck.position, k_GroundedRadius, m_WhatIsGround);
			for (int i = 0; i < colliders.Length; i++)
			{
				if (colliders[i].gameObject != gameObject)
					m_Grounded = true;
			}
			m_Anim.SetBool("Ground", m_Grounded);

			// Set the vertical animation
			m_Anim.SetFloat("vSpeed", m_Rigidbody2D.velocity.y);

			m_Rigidbody2D.gravityScale = Math.Min(3.0f/((Math.Max(1+ m_GravityScaleWithHeight * transform.position.y,1.0f))),3.0f);
			//m_Rigidbody2D.gravityScale = 3.0f / m_GravityScaleWithHeight * (transform.position.y + 1);
		}

		public void Move(float move, float movej, float speed)
		{
			//print ("i'm moving :333 " + move);// + ", " + crouch + ", " + jump);
			//only control the player if grounded or airControl is turned on
			if (m_Grounded || m_AirControl)
			{
				// Reduce the speed if crouching by the crouchSpeed multiplier

				// The Speed animator parameter is set to the absolute value of the horizontal input.
				m_Anim.SetFloat("Speed", Mathf.Abs(move));

				// Move the character
				float vertMax = m_MaxSpeed * speed;
				if(move !=0)
					move = Mathf.Sign(move);
				if(movej !=0)
					movej = Mathf.Sign(movej);
				m_Rigidbody2D.velocity = new Vector2(move*vertMax, movej*vertMax);

				// If the input is moving the player right and the player is facing left...
			}
			// If the player should jump...
		}
	}
}