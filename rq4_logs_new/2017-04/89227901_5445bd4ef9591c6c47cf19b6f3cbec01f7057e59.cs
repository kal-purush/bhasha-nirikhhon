using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Ctrl : MonoBehaviour {
    Vector3 mouseStart;
    bool mouseDown = false;
    float moveRate = 0.25f;
    Vector3 xaxis = new Vector3(0, 1, 0);
    Vector3 yaxis = new Vector3(1, 0, 0);
    void Start () {
		
	}
	
	void Update () {
		
	}
    void FixedUpdate()
    {

        if (Input.GetMouseButton(0))
        {
            mouseDown = true;
        }
        else
        {
            mouseDown = false;
        }
        if (mouseDown)
        {
            Vector3 mouseDelta = Input.mousePosition - mouseStart;
            Vector3 campos = Camera.main.gameObject.transform.position;
            Quaternion camrot = Camera.main.gameObject.transform.rotation;
            Quaternion rotx = Quaternion.AngleAxis(mouseDelta.x * moveRate, xaxis);
            Quaternion roty = Quaternion.AngleAxis(-mouseDelta.y * moveRate, yaxis);
            Quaternion rot = rotx * roty;
            Camera.main.gameObject.transform.position = rot * campos;
            Camera.main.gameObject.transform.rotation = rot * camrot;
            xaxis = rot * xaxis;
            yaxis = rot * yaxis;
        }
        mouseStart = Input.mousePosition;
        
        if (Input.GetAxis("Mouse ScrollWheel") < 0) // back
        {
            Camera.main.transform.position *= 1.25f;
        }
        if (Input.GetAxis("Mouse ScrollWheel") > 0) // forward
        {
            Camera.main.transform.position *= 0.8f;
        }
    }
}