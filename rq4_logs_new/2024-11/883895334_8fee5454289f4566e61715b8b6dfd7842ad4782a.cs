using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class NewBehaviourScript : MonoBehaviour
{

    [Header ("Keybinds")]

    public KeyCode jumpKey = KeyCode.Space;

    [Header("Player Movement")]
    public float moveSpeed;
    public Transform orientation;
    public float jumpForce;
    public float jumpCooldown;
    public float airMultiplier;
    bool CanJump;

    public float groundDrag;
    float horizontalInput;
    float verticalInput;
    Vector3 moveDirection;
    Rigidbody rb;

    [Header("Ground Check")]
    public float playerHeight;
    public LayerMask WhatIsGround;
    bool grounded;
    // Start is called before the first frame update
    void Start()
    {
        rb = GetComponent<Rigidbody>();
        rb.freezeRotation = true;
        CanJump = true;
    }

    private void Update()
    {
        
        grounded = Physics.Raycast(transform.position, Vector3.down, playerHeight * 0.5f + 0.2f, WhatIsGround);
        MyInput();
        SpeedControl();

        //groundDrag settings 
        if (grounded)
            rb.drag = groundDrag;
        else
            rb.drag = 0;
    }

    private void FixedUpdate()
    {
        MovePlayer();
    }

    private void MyInput()
    {
        horizontalInput = Input.GetAxisRaw("Horizontal");
        verticalInput = Input.GetAxisRaw("Vertical");

        if(Input.GetKey(jumpKey) && CanJump && grounded){

            CanJump = false;
            Jump();

            Invoke(nameof(resetJump), jumpCooldown);
        }
    }

    private void MovePlayer()
    {
        moveDirection = orientation.forward * verticalInput + orientation.right * horizontalInput;
        
        if(grounded)
            rb.AddForce(moveDirection.normalized * moveSpeed * 10f, ForceMode.Force);

        else if(!grounded)
        {
            rb.AddForce(moveDirection.normalized * moveSpeed * 10f *airMultiplier, ForceMode.Force);
        }

    }

    private void SpeedControl()
    {
        Vector3 vel = new Vector3(rb.velocity.x, 0f, rb.velocity.z);

       
        if (vel.magnitude > moveSpeed)
        {
             Vector3 velLimit = vel.normalized * moveSpeed;
             rb.velocity = new Vector3(velLimit.x, rb.velocity.y, velLimit.z);
        } 
        
    }

    private void Jump()
    {
        rb.velocity = new Vector3(rb.velocity.x, 0f, rb.velocity.z);

        rb.AddForce(transform.up * jumpForce, ForceMode.Impulse);
    }

    private void resetJump()
    {
        CanJump = true;
    }
    
}