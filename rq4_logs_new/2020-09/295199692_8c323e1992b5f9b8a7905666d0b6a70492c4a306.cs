using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SocialPlatforms;

public class PlayerController : MonoBehaviour
{
    private Rigidbody2D rb;
   
    public PlayerConfiguration config;
    public Transform groundCheck;

    void Start()
    {
        rb = GetComponent<Rigidbody2D>();
    }

    void Update()
    {
        if (Physics2D.Linecast(transform.position, groundCheck.position, 1 << LayerMask.NameToLayer("Ground")))
        {
            config.isGrounded = true;
        }
        else
        {
            config.isGrounded = false;
        }

        float horz = Input.GetAxis("Horizontal") * config.moveSpeed * Time.deltaTime;
        rb.velocity = new Vector2(horz, rb.velocity.y);

        if (Input.GetKey(KeyCode.Space) && config.isGrounded == true)
        {
            rb.velocity = new Vector2(rb.velocity.x, config.jumpForce);
        }
    }
}