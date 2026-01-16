using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ShipScript : MonoBehaviour
{
    public Rigidbody2D rb;

    // Use this for initialization
    void Start()
    {

    }

    // Update is called once per frame
    void Update()
    {

    }

    void OnCollisionEnter(Collision collision)
    {
        if(collision.collider.tag == "Wave")
        {
            rb.AddForce(transform.up * 5);
        }
    }

    void OnTriggerEnter2D(Collider2D other)
    {
        float force = other.gameObject.GetComponent<WaveExpand>().magnitude;

        if (other.gameObject.tag == "Wave")
        {
            // Calculate Angle Between the collision point and the player
            Vector3 dir = transform.position - other.transform.position;
            dir = dir.normalized;
            GetComponent<Rigidbody2D>().AddForce(dir * force);
        }
    }
}