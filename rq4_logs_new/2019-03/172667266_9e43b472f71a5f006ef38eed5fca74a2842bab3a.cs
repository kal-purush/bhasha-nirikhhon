using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PickUpTrigger : MonoBehaviour {

	  public float speed;

    private Rigidbody2D rb;

    void Start ()
    {
        rb = GetComponent<Rigidbody2D>();
    }

    void FixedUpdate ()
    {
        float moveHorizontal = Input.GetAxis ("Horizontal");
        float moveVertical = Input.GetAxis ("Vertical");

        Vector3 movement = new Vector3 (moveHorizontal, 0.0f, moveVertical);

        rb.AddForce (movement * speed);
    }

    void OnTriggerEnter2D(Collider2D other) 
    {
	Debug.Log ("Collider"+other.name+" "+gameObject.name);
        if (other.gameObject.CompareTag ("Diamond"))
        {
            other.gameObject.SetActive (false);

        }
    }
}