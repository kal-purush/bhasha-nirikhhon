using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerMovement : MonoBehaviour
{
   public float moveSpeed = 5f;
   public float jumpForce = 5f;
   private int jumpCount = 0;
   private int maxJumpCount = 2;
   private Rigidbody2D rb;

   // Start is called before the first frame update
   void Start()
   {
       rb = GetComponent<Rigidbody2D>();
   }

   // Update is called once per frame
   void Update()
   {
       float moveX = Input.GetAxis("Horizontal");

       Vector2 move = new Vector2(moveX * moveSpeed, rb.velocity.y);
       rb.velocity = move;

       if (Input.GetButtonDown("Jump") && jumpCount < maxJumpCount)
       {
           Jump();
       }
   }

   void Jump()
   {
       rb.AddForce(new Vector2(0f, jumpForce), ForceMode2D.Impulse);
       jumpCount++;
   }

   private void OnCollisionEnter2D(Collision2D collision)
   {
       if (collision.gameObject.CompareTag("Ground"))
       {
           jumpCount = 0;
       }
   }
}