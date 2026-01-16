using System;
using System.Collections;
using System.Collections.Generic;
using Unity.VisualScripting;
using UnityEngine;

public enum BallType { Yellow, White, Blue };
public enum BallState { InSpot, Picked, Thrown };

public class Ball : MonoBehaviour
{
    public int MaxBounceCount;
    public BallType Type;

    public int currentBounceCount = 0;
    public GameObject pickupSpot;
    public BallState state = BallState.InSpot;

    // Start is called before the first frame update
    void Start()
    {
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    public void OnPickup()
    {
        state = BallState.Picked;
        GetComponent<Rigidbody>().constraints = RigidbodyConstraints.None;
    }

    public void OnThrow()
    {
        state = BallState.Thrown;
    }

    public void ReturnToSpot()
    {
        transform.SetPositionAndRotation(pickupSpot.GetComponent<PickupSpot>().GetBallPickupPosition(), Quaternion.identity);
        currentBounceCount = 0;
        state = BallState.InSpot;
        GetComponent<Rigidbody>().constraints = RigidbodyConstraints.FreezePosition;
    }

    void OnCollisionEnter(Collision collision)
    {
        var collider = collision.collider;

        if (state == BallState.Thrown)
        {
            if (collider.CompareTag("Surface"))
            {
            }
            currentBounceCount += 1;

            if (currentBounceCount == MaxBounceCount)
            {
                ReturnToSpot();
            }
        }
    }

    void OnCollisionExit(Collision collision)
    {
        var collider = collision.collider;

        if (collider.CompareTag("Surface"))
        {
        }
    }
}