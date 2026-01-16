using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GDMovement : MonoBehaviour
{
    private Rigidbody2D rb;
    private bool canJump = false;
    private bool canDrop = false;

    public GameObject raycast;
    public float SPEED_X = 5.0f;
    public float SPEED_Y = 10.0f;

    // Start is called before the first frame update
    void Start()
    {
        rb = GetComponent<Rigidbody2D>();
    }

    void Update()
    {
        rb.velocity = new Vector2(0, rb.velocity.y);

        if (Input.GetKey(KeyCode.LeftArrow))
        {
            rb.velocity = new Vector2(-SPEED_X, rb.velocity.y);
        }

        if (Input.GetKey(KeyCode.RightArrow))
        {
            rb.velocity = new Vector2(SPEED_X, rb.velocity.y);
        }
        if (Input.GetKey(KeyCode.UpArrow) && canJump)
        {
            canJump = false;
            canDrop = true;
            rb.velocity = new Vector2(rb.velocity.x, SPEED_Y);
        }
        if (Input.GetKey(KeyCode.DownArrow) && canDrop)
        {
            canDrop = false;
            rb.velocity = new Vector2(rb.velocity.x, -SPEED_Y);
        }

        RaycastHit2D hit = Physics2D.Raycast(raycast.transform.position, -Vector2.up, 0.1f);

        if (hit)
            canJump = true;

        Debug.DrawRay(raycast.transform.position, -Vector2.up * 0.1f, Color.red);
    }
}