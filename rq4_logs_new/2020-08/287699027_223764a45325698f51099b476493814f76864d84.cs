using UnityEngine;

public class FlyCamera : MonoBehaviour
    {
    float cameraSpeed = 40f;
    void Update()
        {
        Vector3 p = GetBaseInput();
        p *= Time.deltaTime;
        Vector3 newPosition = transform.position;
        transform.Translate(p);
        newPosition.x = transform.position.x;
        newPosition.z = transform.position.z;
        transform.position = newPosition;
        }

    private Vector3 GetBaseInput()
        {
        Vector3 p_Velocity = new Vector3();
        if (Input.GetKey(KeyCode.W))
            {
            p_Velocity += new Vector3(0, 0, cameraSpeed);
            }
        if (Input.GetKey(KeyCode.S))
            {
            p_Velocity += new Vector3(0, 0, -cameraSpeed);
            }
        if (Input.GetKey(KeyCode.A))
            {
            p_Velocity += new Vector3(-cameraSpeed, 0, 0);
            }
        if (Input.GetKey(KeyCode.D))
            {
            p_Velocity += new Vector3(cameraSpeed, 0, 0);
            }
        return p_Velocity;
        }
    }