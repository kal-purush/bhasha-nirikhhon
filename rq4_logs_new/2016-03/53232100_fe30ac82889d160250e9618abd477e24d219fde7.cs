using UnityEngine;
using System.Collections;

public class PlayerController : MonoBehaviour {

	public float speed = 100;
    private Rigidbody2D rigi;	

	void Start()
	{
        rigi = GetComponent<Rigidbody2D> ();
	}

	void FixedUpdate()
	{
		float moveHorizontal = Input.GetAxis ("Horizontal");
		float moveVertical = Input.GetAxis ("Vertical");
        rigi.AddForce(new Vector2(moveHorizontal * speed, moveVertical * speed));
        if(rigi.velocity.x!=0|| rigi.velocity.y != 0) rigi.AddForce(new Vector2(-rigi.velocity.x*10, -rigi.velocity.y*10));
    }

	void OnTriggerEnter2D(Collider2D other) 
	{
	}
}