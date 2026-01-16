using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BulletMove : MonoBehaviour {

    public float bulletSpeed;
	// Use this for initialization
	void Start ()
    {
        StartCoroutine(Life());
	}
	
	// Update is called once per frame
	void Update () {

        transform.position = transform.position + (Vector3.right * Time.deltaTime * bulletSpeed);

		
	}

    IEnumerator Life()
    {
        yield return new WaitForSeconds(1f);
        Destroy(gameObject);
    }

    private void OnCollisionEnter2D(Collision2D collision)
    {
        if (collision.gameObject.tag == "Player")
        {
            Physics2D.IgnoreCollision(collision.gameObject.GetComponent<Collider2D>(), gameObject.GetComponent<Collider2D>());
        }
        else
        {
            Destroy(gameObject);
        }
    }
}