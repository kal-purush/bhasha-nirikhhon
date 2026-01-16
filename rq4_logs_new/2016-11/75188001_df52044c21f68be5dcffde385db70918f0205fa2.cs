using UnityEngine;
using System.Collections;

public class PlayerController : MonoBehaviour {

    private new Rigidbody2D rigidbody;
    [SerializeField]
    private float speed;
    [SerializeField]
    private Transform[] groundpoints;
    [SerializeField]
    private Transform[] wallCheck;
    [SerializeField]
    private float groundRadius;
    [SerializeField]
    private float wallRadius;
    [SerializeField]
    private LayerMask ground;
    [SerializeField]
    private LayerMask wall;
    [SerializeField]
    private float jumpForce;
    private bool isGrounded;
    private bool jumping;
    private bool walled;

    //Gets the Rigidbody component of the player.
	void Start () {
        rigidbody = GetComponent<Rigidbody2D>();
	}

    //Manages wheather the player has activated certain imputs.
    private void HandleInput()
    {
        if (Input.GetKeyDown(KeyCode.Space))
        {
            jumping = true;
        }
    }
	
    //Runs functions that handle movement and jumping.
	void FixedUpdate () {
        float horizontal = Input.GetAxis("Horizontal");
        HandleMovement(horizontal);
        isGrounded = IsGrounded();
        walled = WallCheck();
        ResetValues();
    }

    //Runs "HandleInput."
    private void Update()
    {
        HandleInput();
    }

    //Checks to see if the player has or has not completed the level.
    private void HandleMovement(float horizontal)
    {
        if (!gameObject.GetComponent<Triggers>().levelComplete)
        {
            //Handles vertical movement.
            if (walled == false)
            {
                rigidbody.velocity = new Vector2(horizontal * speed, rigidbody.velocity.y);
            }

            //Handles jumping.
            if (isGrounded && jumping)
            {
                isGrounded = false;
                rigidbody.AddForce(new Vector2(0, jumpForce));
            }
        }
        else
        {
            rigidbody.velocity = new Vector2(0.0f, 0.0f);
        }
    }

    //Checks to see if the player is grounded.
    private bool IsGrounded()
    {
        if (rigidbody.velocity.y <= 0.2)
        {
            foreach (Transform point in groundpoints)
            {
                Collider2D[] colliders = Physics2D.OverlapCircleAll(point.position, groundRadius, ground);
                for (int i = 0; i < colliders.Length; i++)
                {
                    if (colliders[i].gameObject != gameObject)
                    {
                        //If the above is true (a groundpoint collides with something deemed "ground")...
                        return true;
                    }
                }
            }
        }
        //If not...
        return false;
    }

    //Checks to see if the player is touching a wall.
    private bool WallCheck()
    {
        if (rigidbody.velocity.y <= 0.2)
        {
            foreach (Transform point in wallCheck)
            {
                Collider2D[] colliders = Physics2D.OverlapCircleAll(point.position, wallRadius, wall);
                for (int i = 0; i < colliders.Length; i++)
                {
                    if (colliders[i].gameObject != gameObject)
                    {
                        //If the above is true (a wall point collides with something deemed "wall")...
                        return true;
                    }
                }
            }
        }
        //If not...
        return false;
    }

    //Resets values of the player.
    private void ResetValues()
    {
        jumping = false;
        if (isGrounded == true)
        {
            walled = false;
        }
    }
}