using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class player_behavior : MonoBehaviour
{
    //variables

    //speed variables
    public float speed;
    float Move_X;
    float Move_Y;
    private Rigidbody2D rb; 
    // Start is called before the first frame update
    void Start()
    {
        rb = GetComponent<Rigidbody2D>();
    }

    // Update is called once per frame
    void Update()
    {
        //move left or right
        Move_X = Input.GetAxis("Horizontal");
        Move_Y = Input.GetAxis("Vertical");
        Quaternion rotation = Quaternion.Euler(0f, 0f, -40f);
        Vector2 movement = new Vector2(speed * Move_X , speed * Move_Y).normalized;
        movement = rotation * movement;
        rb.velocity = movement;
        
    } 
}

/*
public class player_behavior : MonoBehaviour
{
    public float speed = 5f; // Adjust the speed as needed
    private Rigidbody2D rb;

    void Start()
    {
        rb = GetComponent<Rigidbody2D>();
    }

    void Update()
    {
        float move_X = Input.GetAxis("Horizontal");
        float move_Y = Input.GetAxis("Vertical");

        // Create a quaternion rotation based on 45 degrees
        Quaternion rotation = Quaternion.Euler(0f, 0f, -45f);

        // Create a vector representing the movement input
        Vector2 movement = new Vector2(speed * move_X, speed * move_Y);

        // Rotate the movement vector by 45 degrees
        movement = rotation * movement;

        // Set the player's velocity based on the rotated movement vector
        rb.velocity = movement;

        Debug.Log($"Player Velocity: {rb.velocity}");
    }
}*/