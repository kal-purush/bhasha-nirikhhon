using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EXplayerMove : MonoBehaviour
{
    Rigidbody2D rb;
    public float speed = 5;

    // Start is called before the first frame update
    void Start()
    {
        rb = GetComponent<Rigidbody2D>();
    }

    // Update is called once per frame
    void Update()
    {
        Move();
    }

    private void Move()
    {
        Vector2 pMove = new Vector2();
        if (Input.GetKey(KeyCode.RightArrow))
        {
            pMove = Vector2.right * speed;
        }
        else if (Input.GetKey(KeyCode.LeftArrow))
        {
            pMove = Vector2.left * speed;
        }
        rb.velocity = pMove;
    }
}