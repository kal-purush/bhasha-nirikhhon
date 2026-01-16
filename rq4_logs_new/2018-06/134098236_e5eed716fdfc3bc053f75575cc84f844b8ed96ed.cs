using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnemyBulletMove : MonoBehaviour
{

    public float bulletSpeed;

    // Use this for initialization
    void Start()
    {
        //bullets have a total life time based on spawn time
        StartCoroutine(Life());
    }

    // Update is called once per frame
    void Update()
    {
        transform.position = transform.position + (Vector3.left * Time.deltaTime * bulletSpeed);
    }
    //Keeps the bullet alive for a certain amount of time, then destroys it
    IEnumerator Life()
    {
        yield return new WaitForSeconds(.8f);
        Destroy(gameObject);
    }

    private void OnCollisionEnter2D(Collision2D collision)
    {
        //bullets ignore player collision
        if (collision.gameObject.tag == "Enemy")
        {
            Physics2D.IgnoreCollision(collision.gameObject.GetComponent<Collider2D>(), gameObject.GetComponent<Collider2D>());
        }
        else
        {
            //bullets also will die on colliding with objects 
            Destroy(gameObject);
        }
    }
}