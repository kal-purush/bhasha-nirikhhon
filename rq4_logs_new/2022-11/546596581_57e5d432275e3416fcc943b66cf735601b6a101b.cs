using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Ladder_Climb : MonoBehaviour
{

    private float vertical;
    private float speed = 8f;

    private bool isClimbing;
    private bool isLadder;

    [SerializeField] private Rigidbody2D rb;

    public Animator animator;

    // Update is called once per frame
    void Update()
    {
        vertical = Input.GetAxis("Vertical");

        if (isLadder && Mathf.Abs(vertical) > 0)
        {
            isClimbing = true;
            animator.SetBool("IsClimbing",true);
        }
    }

    private void FixedUpdate()
    {
        if (isClimbing)
        {
            rb.gravityScale = 0f;
            rb.velocity = new Vector2(rb.velocity.x, vertical * speed);
            animator.SetFloat("VerticalSpeed", Mathf.Abs(rb.velocity.y));
        }
        else
        {
            rb.gravityScale = 4f;
        }
    }

    private void OnTriggerEnter2D(Collider2D collision)
    {
        if (collision.CompareTag("Ladder"))
        {
            isLadder = true;    

        }
        
    }

    private void OnTriggerExit2D(Collider2D collision)
    {
        if (collision.CompareTag("Ladder"))
        {
            isLadder = false;
            isClimbing = false;
            animator.SetBool("IsClimbing", false);

        }
        
    }

}