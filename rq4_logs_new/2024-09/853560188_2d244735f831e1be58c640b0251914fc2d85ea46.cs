using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class circleMove : MonoBehaviour
{
    // Start is called before the first frame update

    private float speed = 10f;
    public bool moveRight;
    public Transform pointA, pointB;

    public Transform target;
    public KeyCode space;
    void Start()
    {
        //target = GameObject.Find("line").transform;
    }

    // Update is called once per frame
    void FixedUpdate()
    {
        if (moveRight)
        {
            transform.position += new Vector3(speed * Time.deltaTime, 0, 0);
        }
        else
        {
            transform.position -= new Vector3(speed * Time.deltaTime, 0, 0);
        }

        if (transform.position.x <= pointB.position.x)
        {
            moveRight = true;
        }
        if(transform.position.x >= pointA.position.x)
        {
            moveRight = false;
        }
    }

    void OnTriggerEnter2D(Collider2D col)
    {
        if (Input.GetKeyDown(space))
        {
            Debug.Log("yay");
        }
    }

    
}