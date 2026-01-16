using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class IgnoreRope : MonoBehaviour
{
    private void OnCollisionEnter2D(Collision2D collision)
    {

        if (collision.gameObject.tag == "Rope" || collision.gameObject.tag == "RopeCore")
        {
            Physics2D.IgnoreCollision(collision.gameObject.GetComponent<BoxCollider2D>(), gameObject.GetComponent<BoxCollider2D>());
        }
        if (collision.gameObject.tag == "Crab1" || collision.gameObject.tag == "Crab2")
        {
            Physics2D.IgnoreCollision(collision.gameObject.GetComponent<PolygonCollider2D>(), gameObject.GetComponent<PolygonCollider2D>());
        }

    }
}