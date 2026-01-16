using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CamController : MonoBehaviour
{
    public Transform robotTarget; // Assign the target GameObject in the inspector
    public float moveSpeed = 1.0f;
    public float zoomSpeed = 1.0f;
    public float rotationSpeed = 35.0f;

    private float distanceToTarget;

    private void Start()
    {
        // Initialize distance to target
        if (robotTarget != null)
        {
            distanceToTarget = Vector3.Distance(transform.position, robotTarget.position);
        }
    }

    void Update()
    {
        // Vertical movement on the Y axis
        if (Input.GetKey(KeyCode.UpArrow) && !Input.GetKey(KeyCode.Space))
        {
            MoveInXAndY(Vector3.up);
        }
        if (Input.GetKey(KeyCode.DownArrow) && !Input.GetKey(KeyCode.Space))
        {
            MoveInXAndY(Vector3.down);
        }

        // Horizontal movement on the X axis
        if (Input.GetKey(KeyCode.LeftArrow) && !Input.GetKey(KeyCode.Space))
        {
            MoveInXAndY(Vector3.left);
        }
        if (Input.GetKey(KeyCode.RightArrow) && !Input.GetKey(KeyCode.Space))
        {
            MoveInXAndY(Vector3.right);
        }

        // Zoom in, zoom out
        if (Input.GetKey(KeyCode.Return))
        {
            Zoom(zoomSpeed);
        }
        if (Input.GetKey(KeyCode.RightShift))
        {
            Zoom(-zoomSpeed);
        }

        // Rotation around the Y-axis
        if (Input.GetKey(KeyCode.RightArrow) && Input.GetKey(KeyCode.Space))
        {
            RotateAroundAxis(Vector3.up, rotationSpeed);
        }
        if (Input.GetKey(KeyCode.LeftArrow) && Input.GetKey(KeyCode.Space))
        {
            RotateAroundAxis(Vector3.up, -rotationSpeed);
        }

        // Rotation around the X-axis
        if (Input.GetKey(KeyCode.UpArrow) && Input.GetKey(KeyCode.Space))
        {
            RotateAroundAxis(Vector3.right, rotationSpeed);
        }
        if (Input.GetKey(KeyCode.DownArrow) && Input.GetKey(KeyCode.Space))
        {
            RotateAroundAxis(Vector3.right, -rotationSpeed);
        }
    }

    void MoveInXAndY(Vector3 direction)
    {
        transform.Translate(direction * moveSpeed * Time.deltaTime, Space.World);
    }

    void Zoom(float speed)
    {
        Vector3 zoomDirection = Vector3.zero;
        zoomDirection = transform.forward * speed * Time.deltaTime;
        transform.Translate(zoomDirection, Space.World);
        distanceToTarget = Vector3.Distance(transform.position, robotTarget.position); // Update distance after zoom
    }

    void RotateAroundAxis(Vector3 axis, float speed)
    {
        if (robotTarget != null)
        {
            // Keep the camera facing the target
            transform.RotateAround(robotTarget.position, axis, speed * Time.deltaTime);
            transform.LookAt(robotTarget);

            // Adjust camera's position to maintain current distance to the target
            Vector3 directionNormalized = (transform.position - robotTarget.position).normalized;
            transform.position = robotTarget.position + directionNormalized * distanceToTarget;
        }
    }

}