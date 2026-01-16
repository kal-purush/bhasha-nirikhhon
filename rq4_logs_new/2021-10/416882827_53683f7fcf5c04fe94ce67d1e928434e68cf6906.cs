using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CharController : MonoBehaviour
{

    public CharacterController controller;

    public float speed = 18f; //Speed at which the player walks around.
    public float gravity = -100.81f;
    public float jumpHeight = 6f;

    public float turnSmoothTime = 0.1f; //Speed at which the player turns (rotates) to face the direction he is moving in.
    public float turnSmoothVelocity;

    public Transform groundCheck;
    public float groundDistance = 0.4f; //This is the radius of a sphere that is projected from the player's feet. It checks to see if the ground is anywhere within this sphere. 
    public LayerMask groundMask; //This controls what objects the sphere should check for, so it doesn't collide with the player.
    bool isGrounded;

    
    Vector3 velocity; //This is the velocity that controls how gravity effects the player over time, increasing the speed at which he falls, etc. 
    
    void Update()
    {
        isGrounded = Physics.CheckSphere(groundCheck.position, groundDistance, groundMask);

        if (isGrounded && velocity.y < 0)
        {
            velocity.y = -2f;
        }

        float horizontal = Input.GetAxisRaw("Horizontal");
        float vertical = Input.GetAxisRaw("Vertical");
        Vector3 direction = new Vector3(-vertical, 0f, horizontal).normalized;

        if (direction.magnitude >= 0.1f)
        {
            float targetAngle = Mathf.Atan2(direction.x, direction.z) * Mathf.Rad2Deg;
            float angle = Mathf.SmoothDampAngle(transform.eulerAngles.y, targetAngle, ref turnSmoothVelocity, turnSmoothTime);
            transform.rotation = Quaternion.Euler(0f, angle, 0f);

            controller.Move(direction * speed * Time.deltaTime);
        }

        if(Input.GetButtonDown("Jump") && isGrounded)
        {
            velocity.y = Mathf.Sqrt(jumpHeight * -2f * gravity);
        }

        velocity.y += gravity * Time.deltaTime;
        controller.Move(velocity * Time.deltaTime); //increases fall speed over time.
    }
}