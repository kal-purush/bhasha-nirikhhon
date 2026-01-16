using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ProjectileSplit : MonoBehaviour {

	public int lifetime = 6;

	void Start()
	{
		Invoke("Destroy", lifetime);
	}

	private void Destroy()
	{
		Destroy(gameObject);
	}

	private void OnTriggerEnter2D(Collider2D col)
    {
        if(col.gameObject.name == "Ice Ball(Clone)")
        {
            GameObject gm1 = Instantiate(col.gameObject, col.gameObject.transform.position, Quaternion.identity);
            GameObject gm2 = Instantiate(col.gameObject, col.gameObject.transform.position, Quaternion.identity);

            gm1.GetComponent<Rigidbody2D>().velocity = col.gameObject.GetComponent<Rigidbody2D>().velocity;
            gm2.GetComponent<Rigidbody2D>().velocity = col.gameObject.GetComponent<Rigidbody2D>().velocity;
        
            //Vector2 direction = cursorInWorldPos - new Vector2(transform.position.x, transform.position.y);
            //fb.GetComponent<Rigidbody2D>().velocity = (direction + new Vector2(tempX, 0)) * atkSpeed;
        }
        else if(col.gameObject.tag == "PlayerGolem")
        {
            
        }
    }
}