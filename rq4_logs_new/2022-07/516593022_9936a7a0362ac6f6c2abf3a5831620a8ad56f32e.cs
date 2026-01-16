using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class WallRun : MonoBehaviour
{
    [Header("Wall Running Setting")]
    public LayerMask isWall;
    public LayerMask isGround;
    public float wallRunForce, maxWallrunTime, maxWallSpeed;

    [Header("Input")]
    private float horizontalInput, verticalInput;

    [Header("Detection")]
    public float wallCheckDistance, minJunpHeight;
    private bool isWallRight, isWallLeft;
    private RaycastHit leftWall, rightWall;

    [Header("Reference")]
    public Transform orientation;
    private Rigidbody rb;

    private void Start()
    {
        rb = GetComponent<Rigidbody>();
    }
}