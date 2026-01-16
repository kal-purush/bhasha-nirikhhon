using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlatformController : MonoBehaviour
{
    public PlatformConfiguration config;

    private void Start()
    {
        //Set Center Position
        config.centerPos = transform.position;
    }

    private void FixedUpdate()
    {
        //Check Direction
        switch (config.direction)
        {
            case PlatformState.Horizontal:
                MoveHorizontal();
                break;
            case PlatformState.Vertical:
                MoveVertical();
                break;
            case PlatformState.Diagonal:
                MoveDiagonally();
                break;
        }
    }

    private void MoveHorizontal()
    {
        //Check Range
        if (GetDistance() >= config.rangeX)
        {
            config.moveSpeed = config.moveSpeed * -1;
        }

        //Move Position
        transform.position += new Vector3(config.moveSpeed * Time.deltaTime, 0, 0);
    }

    private void MoveVertical()
    {
        //Check Range
        if (GetDistance() >= config.rangeY)
        {
            config.moveSpeed = config.moveSpeed * -1;
        }

        //Move Position
        transform.position += new Vector3(0, config.moveSpeed * Time.deltaTime, 0);
    }


    private void MoveDiagonally()
    {
        //Check Range
        if (GetDistance() >= config.rangeX || GetDistance() >= config.rangeY)
        {
            config.moveSpeed = config.moveSpeed * -1;
        }

        //Move Position
        transform.position += new Vector3(config.moveSpeed * Time.deltaTime, config.moveSpeed * Time.deltaTime, 0);
    }


    private float GetDistance()
    {
        return Vector3.Distance(config.centerPos, transform.position);
    }

    private void OnCollisionEnter2D(Collision2D collision)
    {
        //This Prevents Slipping and Jumpy movement.
        if (collision.gameObject.tag == "Player")
        {
            collision.collider.transform.SetParent(transform);
        }
    }

    private void OnCollisionExit2D(Collision2D collision)
    {
        //This Prevents Slipping and Jumpy movement.
        collision.collider.transform.SetParent(null);
    }
}