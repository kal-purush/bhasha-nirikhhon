using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class FollowPlayer : MonoBehaviour
{
    
    public Rigidbody2D playerRb;
    /*
    public float dampingFactor = 0.1f;
    public float maxSpeed = 500.0f;
    public float maxLeadingDistance = 5.0f;
    public float minTime = 1f;
    public float minMagnitude = 0.01f;
    
    private Vector3 zeroVector = Vector3.zero;
    */
    public float cameraVelocityFactor = 0.2f;
    public float playerVelocityFactor = 1.2f;
    public float maxAheadDistance = 2;
    private Vector3 playerLastPosition;

    private void Start()
    {
        playerLastPosition = playerRb.position;
    }

    void Update()
    {

    }
}