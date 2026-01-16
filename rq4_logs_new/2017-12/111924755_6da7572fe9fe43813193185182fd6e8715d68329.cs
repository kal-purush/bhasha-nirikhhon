using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Player : MonoBehaviour
{
    public AudioClip jump;

    Rigidbody2D body;

    int speed = 5;

	void Start ()
    {
        body = GetComponent<Rigidbody2D>();
	}
	
	void FixedUpdate ()
    {
        body.velocity = new Vector2(Input.GetAxisRaw("Horizontal") * speed, body.velocity.y);

        RaycastHit2D hitA = Physics2D.Raycast(new Vector2(transform.position.x - 0.5f, transform.position.y -0.51f), Vector2.down);
        RaycastHit2D hitB = Physics2D.Raycast(new Vector2(transform.position.x + 0.5f, transform.position.y - 0.51f), Vector2.down);
        if (hitA.distance < 0.01f || hitB.distance < 0.01f)
        {
            if (Input.GetKeyDown(KeyCode.Space))
            {
                SoundHandler.Instance.PlaySound(jump);
                body.AddForce(new Vector2(0, 350));
            }
        }
	}
}