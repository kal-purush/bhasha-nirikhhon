using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BoxMovement : MonoBehaviour
{
    [SerializeField] private int pushSpeed = 5;
    [SerializeField] private bool touchingPlayer;
    [SerializeField] private string tagToPush;

    private void OnCollisionEnter(Collision other)
    {
        if (other.transform.CompareTag(tagToPush))
        {
            touchingPlayer = true;
        }
    }

    private void OnCollisionExit(Collision other)
    {
        if (other.transform.CompareTag(tagToPush))
        {
            touchingPlayer = false;
        }
    }

    void FixedUpdate()
    {
        
    }
}