using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerControl : MonoBehaviour
{
    public float speed = 4f;
    public float gravity = -9.81f;

    private CharacterController charController;

    private Vector3 movement;
    private float ySpeed;

    // Start is called before the first frame update
    void Start()
    {
        charController = GetComponent<CharacterController>();
        ySpeed = 0.0f;
    }

    // Update is called once per frame
    void Update()
    {
        ySpeed += gravity * Time.deltaTime;

        movement = new Vector3(Input.GetAxis("Horizontal"), 0.0f, Input.GetAxis("Vertical"));
        movement *= speed;
        transform.LookAt(transform.position + movement);
        movement.y = ySpeed;
        movement *= Time.deltaTime;

        charController.Move(movement);
        if (charController.isGrounded)
        {
            ySpeed = 0.0f;
        }
    }
}