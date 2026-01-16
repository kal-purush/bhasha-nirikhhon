using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ballScript : MonoBehaviour
{
    public Rigidbody2D rb;
    public bool inPlay;
    public Transform paddle;
    public float speed;

    // Start is called before the first frame update
    void Start()
    {
        rb = GetComponent<Rigidbody2D>();
    }

    // Update is called once per frame
    void Update()
    {
        if (!inPlay)
        {
            transform.position = paddle.position;
        }

        //jump maps to spacebar(trigger)
        if (Input.GetButtonDown("Jump") && !inPlay)
        {
            inPlay = true;
            rb.AddForce(Vector2.up * speed);
        }
    }

    //dead collider(triggers game lose)
    void OnTriggerEnter2D(Collider2D other)
        //needs colliders, @least 1 w. rigid body
    {
        if (other.CompareTag("Bottom"))
        {
            Debug.Log("ball hit bottom of screen");
            rb.velocity = Vector2.zero;
            inPlay = false;
        }
    }
}