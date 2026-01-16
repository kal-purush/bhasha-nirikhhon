using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class fpCamera : MonoBehaviour
{   
    
    public float mouseSpeed = 100f;
    public Transform cameraMovement;
    float Horizontal = 0f;

    // Start is called before the first frame update
    void Start()
    {
        Cursor.lockState = CursorLockMode.Locked;
    }

    // Update is called once per frame
    void Update()
    {
        float mouseX = Input.GetAxis("Mouse X") * mouseSpeed * Time.deltaTime;
        float mouseY = Input.GetAxis("Mouse Y") * mouseSpeed * Time.deltaTime;
        Horizontal -= mouseY;
        Horizontal = Mathf.Clamp(Horizontal, -90f, 90f);
        transform.localRotation = Quaternion.Euler(Horizontal, 0f, 0f);
        cameraMovement.Rotate(Vector3.up * mouseX);
    }
}