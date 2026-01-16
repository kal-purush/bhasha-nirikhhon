using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class LookBehaviour : MonoBehaviour
{

    float mousex;

    float mousey;

    public Transform ThePlayer;

    float xRot = 0f;
    // Start is called before the first frame update
    void Start()
    {
        Cursor.lockState = CursorLockMode.Locked;
    }

    // Update is called once per frame
    void Update()
    {
        mousex = Input.GetAxis("Mouse X") * 100f * Time.deltaTime;
        mousey = Input.GetAxis("Mouse Y") * 100f * Time.deltaTime;

        ThePlayer.Rotate(Vector3.up * mousex);

        xRot -= mousey;

        transform.localRotation = Quaternion.Euler(xRot, 0f, 0f);

        xRot= Mathf.Clamp(xRot, -90f, -90f);
    }
}