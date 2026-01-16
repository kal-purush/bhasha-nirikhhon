using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Projectile : MonoBehaviour
{
    public Vector2 Target;
    public float damage;
    float speed;

    private void Update()
    {
        Vector2.MoveTowards(transform.position, Target, Time.deltaTime * speed);
    }

    private void OnCollisionEnter2D(Collision2D collision)
    {
        if(collision.gameObject.tag == "Player")
        {
            //do damage
        }
        gameObject.transform.position = Vector2.zero;
        this.gameObject.SetActive(false);
    }
}