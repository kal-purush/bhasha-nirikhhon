using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerController : MonoBehaviour {

    private Rigidbody2D rb2d;
    private int SPEED = 500;
	// Use this for initialization
	void Start () {

        rb2d = GetComponent<Rigidbody2D>();

    }
	
	// Update is called once per frame
	void Update () {
		
        if (rb2d != null)
        {
            float xMovement = Input.GetAxis("Horizontal");

            if (xMovement > .2f || xMovement < -.2f)
            {
                rb2d.AddForce(Vector2.right * xMovement * Time.deltaTime * SPEED);
            }

            
        }
            

	}
}