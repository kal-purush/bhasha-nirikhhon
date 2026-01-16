using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CameraRigs : MonoBehaviour
{
    public float Velocity = 10;
    public string PathHierarchy = "MainCamera";
    public Transform CentralBarge;

    private Camera _defaultCamera;
    private Rigidbody _rb;

    // Start is called before the first frame update
    void Start()
    {
        Transform cameraTransform = transform.Find(PathHierarchy);

        if (cameraTransform)
        {
            _defaultCamera = cameraTransform.GetComponent<Camera>();
        }

        if (!gameObject.GetComponent<Rigidbody>())
        {
            _rb = gameObject.AddComponent<Rigidbody>();
            _rb.useGravity = false;
        }
    }


    // Update is called once per frame
    void Update()
    {
        float verticalAxis = Input.GetAxis("Vertical");
        float horizontalAxis = Input.GetAxis("Horizontal");

        if ((Math.Abs(verticalAxis) + Math.Abs(horizontalAxis)) > 0f)
        {
            float signHorizontal = horizontalAxis.CompareTo(0f) == 0 ? 0 : horizontalAxis * Velocity;
            float signVertical = verticalAxis.CompareTo(0f) == 0 ? 0 : verticalAxis * Velocity;
            _rb.velocity = transform.forward * signVertical + transform.right * signHorizontal;

            MoveCentralBarge(_rb.velocity);
        }
        else
        {
            _rb.velocity = Vector3.zero;

            MoveCentralBarge(Vector3.zero);
        }
    }

    private void MoveCentralBarge(Vector3 bargeShift)
    {
        if (CentralBarge)
        {
            Rigidbody centralBargeRigidbody = CentralBarge.GetComponent<Rigidbody>();
            centralBargeRigidbody.velocity = bargeShift;
        }
    }

}