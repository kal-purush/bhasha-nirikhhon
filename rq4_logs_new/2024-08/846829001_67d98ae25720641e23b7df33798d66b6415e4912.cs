using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class bigs : MonoBehaviour
{
    private Rigidbody2D rb;
    private int j = 45;
    private bool bulletFacing = false;

    // Start is called before the first frame update
    void Start()
    {
        // Get the Rigidbody component


        rb = GetComponent<Rigidbody2D>();
        if (gunscript2.right2 == true)
        {
            rb.velocity = Vector3.right * 14;
        }
        if (gunscript2.right2 != true)
        {
            rb.velocity = Vector3.left * 14;
        }
    }

    // Update is called once per frame
    void FixedUpdate()
    {
        j--;

        if (j == 0)
        {
            rb.velocity = rb.velocity * -1;


        }
        if (player2.bulletDirection == true && bulletFacing == false)
        {
            bulletFacing = true;
            rb.velocity = rb.velocity * -1;
        }
        if (bulletFacing == true && player2.bulletDirection == false)
        {
            bulletFacing= false;
        }
    }
    private void OnTriggerEnter2D(Collider2D collision)
    {
        
        if (!collision.gameObject.CompareTag("blue") && !collision.gameObject.CompareTag("wall") && !collision.gameObject.CompareTag("ground") && !collision.gameObject.CompareTag("player2") && !collision.gameObject.CompareTag("bigboomerang"))
        {
            Destroy(collision.gameObject);
            player2.kill = true;
        }
    }
}