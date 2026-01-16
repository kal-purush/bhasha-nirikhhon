using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerFly : MonoBehaviour
{
    [SerializeField] private float speed = 0.02f;
    private Transform transform;
    private Gyroscope gyro;
    private InputController input;
    private Vector3 destination, deviceTilt;
    private Collider2D collider;
    private Rigidbody2D rigidbody;

    private void Start()
    {
        transform = GetComponent<Transform>();
        input = GetComponent<InputController>();
        collider = GetComponent<Collider2D>();
        rigidbody = GetComponent<Rigidbody2D>();
        input.ActivateDeviceTilt();
        collider.enabled = false;
        rigidbody.isKinematic = true;
    }

    private void Update()
    {
        Fly();
    }

    private void Fly()
    {
        deviceTilt = input.GetDeviceTilt();
        destination = transform.position;

        destination +=
            new Vector3(deviceTilt.z, deviceTilt.x, transform.position.z);

        transform.position =
            Vector3.Lerp(transform.position, destination, speed);
    }

    // Disable device tilt on scipt disabled to save io function
    // Reenables normal physics on player
    private void OnDisable()
    {
        input.DeactivateDeviceTilt();
        collider.enabled = true;
        rigidbody.isKinematic = false;
    }
}