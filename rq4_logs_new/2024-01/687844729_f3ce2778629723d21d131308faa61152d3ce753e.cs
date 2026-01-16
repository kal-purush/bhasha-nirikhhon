using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.InputSystem;

public class CameraInputAction : MonoBehaviour
{
    public void OnCameraMove(InputValue value)
    {
        Vector2 input = value.Get<Vector2>();
        if (input != null) { 
            Vector3 cameraPos = Camera.main.transform.position;
            cameraPos += new Vector3(input.x, input.y, 0);
            Camera.main.transform.position = cameraPos;
        }
    }

    public void OnCameraZoom(InputValue value) {
        float input = value.Get<float>();
        Camera.main.orthographicSize = Mathf.Clamp(Camera.main.orthographicSize + input, 2, 10);
    }
}