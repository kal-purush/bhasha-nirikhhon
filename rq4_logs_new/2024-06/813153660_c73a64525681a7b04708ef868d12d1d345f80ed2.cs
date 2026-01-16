using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PinkManMovement : MonoBehaviour
{
    private Rigidbody2D rb;
    private Animator animator;
    private SpriteRenderer sprite;
    private BoxCollider2D boxCollider;

    private float directionX = 0f; // move distance on x-axis
    private enum MovementState { idle, jumping ,running, falling };

    [SerializeField] private float moveSpeed = 10f;   // speed on x-axis(horizontal movement)
    [SerializeField] private float jumpHight = 10f;   // speed on y-axis jumping hight
    [SerializeField] private LayerMask jumpable;

    // Start is called before the first frame update
    void Start()
    {
        rb = GetComponent<Rigidbody2D>();
        animator = GetComponent<Animator>();      // Player's animator
        sprite = GetComponent<SpriteRenderer>();
        boxCollider = GetComponent<BoxCollider2D>();
    }

    // Update is called once per frame
    void Update()
    {
        // speed per unit: 10 , direction : left or right
        // total speed depends on keyboard input
        directionX = Input.GetAxisRaw("Horizontal");
        rb.velocity = new Vector2(directionX * moveSpeed, rb.velocity.y);
        
        // Jump operation
        if (Input.GetButtonDown("Jump") && IsGrounded())
        {
            // speed per unit: 10 , direction : top
            rb.velocity = new Vector2(rb.velocity.x, jumpHight);
        }
        UpdateAnimation();

        // Vector2 : 2D vectors and points, only 2 axises x(horizontal) and y(vertical)
        // Vector3 : 3D vectors and points, x,y & z axis

    }

    private void UpdateAnimation(){
        MovementState movementState;

        // directionX != 0 -> running left or running right
        // directionX = 0 -> Idle
        if(directionX > 0f)
        {
            movementState = MovementState.running;
            // don't reverse canvas -> animation "running right"
            sprite.flipX = false;
        }
        else if (directionX < 0f){
            movementState = MovementState.running;
            // reverse canvas -> animation "running left"
            sprite.flipX = true;
        }
        else
        {
            movementState = MovementState.idle;
        }

        // directionY: movement speed on y-axis
        // directionY>0 -> jumping ; directionY<0 -> falling
        float directionY = rb.velocity.y;
        if( directionY > .1f)
        {
            movementState = MovementState.jumping;
        }
        else if( directionY < -.1f){
            movementState = MovementState.falling;
        }

        // reset parameter "movementState" of Pinkman's animator for next movment animation
        animator.SetInteger("movementState", (int)movementState);
    }

    // check whether Pinkman is on the ground
    // Pinkman is only jumpable, when he is on the ground
    private bool IsGrounded()
    {
        return Physics2D.BoxCast(boxCollider.bounds.center, boxCollider.bounds.size, 0f , Vector2.down , .1f , jumpable);

        /* BoxCast
        whether boxCollider(Pinkman) touch other items(ground)
        boxCollider.bounds.center -> box start point
        boxCollider.bounds.center -> box size
        0f -> touch angle
        Vector2.down -> touching direction
        .1f -> touching distance
        jumpable -> filter
        */
    }
}