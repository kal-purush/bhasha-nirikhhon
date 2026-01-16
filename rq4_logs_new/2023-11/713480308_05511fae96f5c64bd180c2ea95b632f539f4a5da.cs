using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerMovement : MonoBehaviour
{
    public float horizontalInput;
    public float verticalInput;

    public float speed = 1f;

    void Start()
    {

    }

    void Update()
    {
        horizontalInput = Input.GetAxis("Horizontal");
        verticalInput = Input.GetAxis("Vertical");

        Move();
    }

    void Move()
    {
        transform.Translate (new Vector3 (horizontalInput,verticalInput, 0) * speed * Time.deltaTime);
    }
}