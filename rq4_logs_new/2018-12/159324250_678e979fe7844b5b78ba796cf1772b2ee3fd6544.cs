using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class WallJump : MonoBehaviour
{
    public float distance = 1f;
    SimplePlatformController movement;
    public float speed = 2f;
    public bool walljumping;
    // Use this for initialization
    void Start()
    {
        movement = GetComponent<SimplePlatformController>();

    }

    // Update is called once per frame
    void Update()
    {

        RaycastHit2D hit = Physics2D.Raycast(transform.position, Vector2.right * transform.localScale.x, distance);

        if (Input.GetButtonDown("Jump") && !movement.grounded && hit.collider != null)
        {
            {
                GetComponents<SimplePlatformController>().moveForce = new Vector2(speed * hit.normal.x, speed);
                movement.BaseSpeed = speed * hit.x;
                walljumping = true;
                transform.localScale = transform.localScale.x == 1 ? new Vector2(-1, 1) : Vector2.one;
            }
        }
        else if (hit.collider != null && walljumping)
            walljumping = false;

    }

   private void OnCollisionEnter2D(Collision2D col)
    {
        if ((!walljumping || movement.grounded))
            movement.BaceSpeed = 0;
    }

    void OnDrawGizmos()
    {
        Gizmos.Color = Color.Green;

        Gizmos.DrawLine(transform.position, transform.position + Vector3.right * transform.localScale.x, distance);
    }
}