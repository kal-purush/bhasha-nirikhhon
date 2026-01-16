using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CarMovement : MonoBehaviour
{
    public Rigidbody rigidBody;
    public float force = 2f;
    public float torque = 2f;
    public float maxSpeed = 100f;

    // Start is called before the first frame update
    void Start()
    {

    }

    // Update is called once per frame
    void Update()
    {
        ClampSpeed();

        if (Input.GetKey(KeyCode.UpArrow))
        {
            rigidBody.AddForce(transform.forward * force);
        }
        if (Input.GetKey(KeyCode.DownArrow))
        {
            rigidBody.AddForce(transform.forward * -force);
        }

        if (Input.GetKey(KeyCode.LeftArrow))
        {
            transform.Rotate(transform.up * -torque * Time.deltaTime);
        }
        if (Input.GetKey(KeyCode.RightArrow))
        {
            transform.Rotate(transform.up * torque * Time.deltaTime);
        }
    }

    private void ClampSpeed()
    {
        float speed = rigidBody.velocity.magnitude;
        Vector3 normalizedVelocity = Vector3.Normalize(rigidBody.velocity);
        float clampedSpeed = Mathf.Clamp(speed, -maxSpeed, maxSpeed);
        rigidBody.velocity = normalizedVelocity * clampedSpeed;
    }

    public void Stop()
    {
        rigidBody.velocity = Vector3.zero;
    }
}