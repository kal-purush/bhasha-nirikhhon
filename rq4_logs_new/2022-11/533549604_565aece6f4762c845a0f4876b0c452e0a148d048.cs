using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BlockerAI : MonoBehaviour
{
    private Rigidbody2D rb;
    public float speed = 0.5f;
    private float health = 100;
    private void Awake()
    {
        health = 100;
        speed = 0.5f;
        rb = GetComponent<Rigidbody2D>();
    }

    private void Update()
    {
        Move();
        if (health <= 0)
        {
            Destroy(this.gameObject);
        }
    }
    private void Move()
    {
        Vector2 direction = Vector2.right;
        rb.transform.position = new Vector2(rb.transform.position.x + direction.x * Time.deltaTime * speed, rb.transform.position.y);
    }

    public void SpawnBlocker()
    {
        Instantiate(this.gameObject, new Vector2(-5,-3), Quaternion.identity);
    }

    private void OnCollisionEnter2D(Collision2D other)
    {
        if (other.gameObject.tag == "Enemy")
        {
            Destroy(this.gameObject);
        }
    }
}