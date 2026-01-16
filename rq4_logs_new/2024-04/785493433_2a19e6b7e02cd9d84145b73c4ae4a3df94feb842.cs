using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerMovement : MonoBehaviour
{
    [SerializeField] private float m_Speed = 40f;
    [SerializeField] private float m_JumpForce = 300f;[Range(0, .3f)]
    [SerializeField] private float m_MovementSmoothing = .05f;

    public LayerMask GroundLayer;
    public Vector2 boxSize;
    public float castDistance;

    Animator animator;
    Rigidbody2D rb;
    SpriteRenderer sr;
    float horizontalMove = 0f;
    bool jump = false;
    bool grounded = false;
    Vector3 m_Velocity = Vector3.zero;

    void Start() {
        animator = GetComponent<Animator>();
        rb = GetComponent<Rigidbody2D>();
        sr = GetComponent<SpriteRenderer>();
    }

    // Called once per frame
    void Update()
    {
        checkGrounded();
        HorizontalMovementFrame();
        VerticalMovementFrame();
    }

    // Called once per physics update
    void FixedUpdate()
    {
        HorizontalMovementPhysics();
        VerticalMovementPhysics();
    }

    void HorizontalMovementFrame()
    {
        horizontalMove = Input.GetAxis("Horizontal");
        animator.SetFloat("Speed", Mathf.Abs(horizontalMove));
        if (horizontalMove > 0f)
        {
            sr.flipX = false;
        }
        else if (horizontalMove < 0f)
        {
            sr.flipX = true;
        }
    }

    void HorizontalMovementPhysics()
    {
        float hv = horizontalMove * m_Speed * Time.fixedDeltaTime * 10f;
        Vector3 targetVelocity = new Vector2(hv, rb.velocity.y);
        // And then smoothing it out and applying it to the character
        rb.velocity = Vector3.SmoothDamp(rb.velocity, targetVelocity, ref m_Velocity, m_MovementSmoothing);
    }

    void VerticalMovementFrame()
    {
        if (Input.GetButtonDown("Jump") && grounded)
        {
            jump = true;
        }
    }

    void VerticalMovementPhysics()
    {
        if (jump)
        {
            jump = false;
            rb.AddForce(new Vector2(0f, m_JumpForce));
        }
    }

    void checkGrounded()
    {
        if (Physics2D.BoxCast(transform.position, boxSize, 0f, -transform.up, castDistance, GroundLayer))
        {
            grounded = true;
        }
        else
        {
            grounded = false;
        }
        animator.SetBool("IsJumping", !grounded);
    }

    void OnDrawGizmos()
    {
        Gizmos.DrawWireCube(transform.position - transform.up * castDistance, boxSize);
    }
}