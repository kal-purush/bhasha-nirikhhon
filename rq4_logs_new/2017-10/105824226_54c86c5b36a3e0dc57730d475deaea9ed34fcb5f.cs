using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CameraController : MonoBehaviour {

    Vector2 mouseLook, smoothV;
    public float sensitivity = 5.0f, smoothing = 2.0f;
    GameObject character;
    //// Use this for initialization
    //public float mouseSensitivity = 100.0f;
    //public float clampAngle = 80.0f;

    //private float rotY = 0.0f; // rotation around the up/y axis
    //private float rotX = 0.0f; // rotation around the right/x axis

    void Start()
    {
        character = this.transform.parent.gameObject;
        //Vector3 rot = transform.localRotation.eulerAngles;
        //rotY = rot.y;
        //rotX = rot.x;
    }

    void Update()
    {
        var md = new Vector2(Input.GetAxisRaw("Mouse X"),Input.GetAxisRaw("Mouse Y"));

        md = Vector2.Scale(md, new Vector2(sensitivity * smoothing, sensitivity * smoothing));
        smoothV.x = Mathf.Lerp(smoothV.x, md.x, 1f / smoothing);
        smoothV.y = Mathf.Lerp(smoothV.y, md.y, 1f / smoothing);
        mouseLook += smoothV;

        transform.localRotation = Quaternion.AngleAxis(-mouseLook.y, Vector3.right);
        character.transform.localRotation = Quaternion.AngleAxis(mouseLook.x, character.transform.up);
        //float mouseX = Input.GetAxis("Mouse X");
        //float mouseY = -Input.GetAxis("Mouse Y");

        //rotY += mouseX * mouseSensitivity * Time.deltaTime;
        //rotX += mouseY * mouseSensitivity * Time.deltaTime;

        //rotX = Mathf.Clamp(rotX, -clampAngle, clampAngle);

        //Quaternion localRotation = Quaternion.Euler(rotX, rotY, 0.0f);
        //transform.rotation = localRotation;
    }
}