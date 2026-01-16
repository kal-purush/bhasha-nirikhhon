using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class cameraController : MonoBehaviour
{
    public float panSpeed = 20f;
    public float panBorderThickness = 10f;
    public Vector2 panLimit;
    public float ScrollSpeed = 20f;
    public float minScroll = 20f;
    public float maxScroll = 120f;


    // Update is called once per frame
    void Update() {
        
        Vector3 pos = transform.position;
        Vector3 mousePos = Input.mousePosition;

        if (Input.GetKey("w") || mousePos.y >= Screen.height - panBorderThickness) {
            pos.z += panSpeed * Time.deltaTime * (pos.y / 10); 
        }
        if (Input.GetKey("s") || mousePos.y <= panBorderThickness) {
            pos.z -= panSpeed * Time.deltaTime * (pos.y / 10);
        }
        if (Input.GetKey("d") || mousePos.x >= Screen.width - panBorderThickness) {
            pos.x += panSpeed * Time.deltaTime * (pos.y / 10);
        }
        if (Input.GetKey("a") || mousePos.x <= panBorderThickness) {
            pos.x -= panSpeed * Time.deltaTime * (pos.y / 10);
        }

        float scroll = Input.GetAxis("Mouse ScrollWheel");
        pos.y += scroll * ScrollSpeed * -100f * Time.deltaTime;

        pos.x = Mathf.Clamp(pos.x, -panLimit.x, panLimit.x);
        pos.y = Mathf.Clamp(pos.y, minScroll, maxScroll);
        pos.z = Mathf.Clamp(pos.z, -panLimit.y, panLimit.y);

        transform.position = pos;
    }
}