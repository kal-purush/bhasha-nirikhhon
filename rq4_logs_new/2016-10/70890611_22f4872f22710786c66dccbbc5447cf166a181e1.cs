using UnityEngine;
using System.Collections;

public class PlayerController : MonoBehaviour {


    public float speed;
    private Rigidbody2D rb2d;
    private int count;

    // Use this for initialization
    void Start () {
        rb2d = GetComponent<Rigidbody2D>();
        count = 0;
    }
	
	// Update is called once per frame
	void FixedUpdate () {

        //Store the current horizontal input in the float moveHorizontal.
        float moveHorizontal = Input.GetAxis("Horizontal");

        //Store the current vertical input in the float moveVertical.
        //float moveVertical = Input.GetAxis("Vertical");

        //Use the two store floats to create a new Vector2 variable movement.
        Vector2 movement = new Vector2(moveHorizontal, 0.0f);

        //Call the AddForce function of our Rigidbody2D rb2d supplying movement multiplied by speed to move our player.
        rb2d.AddForce(movement * speed);
    }

    //OnTriggerEnter2D is called whenever this object overlaps with a trigger collider.
    void OnTriggerEnter2D(Collider2D other)
    {
        //Check the provided Collider2D parameter other to see if it is tagged "PickUp", if it is...
        if (other.gameObject.CompareTag("PickUp"))

            //... then set the other object we just collided with to inactive.
            other.gameObject.SetActive(false);

        //Add one to the current value of our count variable.
        count = count + 1;
    }
}