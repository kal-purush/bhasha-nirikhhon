using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CameraManager : MonoBehaviour
{
    public float speed = 7f;
    public float border = 30f;

    // Update is called once per frame
    void Update()
    {
        Vector3 pos = transform.position;

        if(Input.mousePosition.y >= Screen.height - border && Input.mousePosition.y <= Screen.height && pos.y < 21)
        {
            pos.y += speed * Time.deltaTime;
        }
        else if (Input.mousePosition.y <= border && Input.mousePosition.y >= 0 && pos.y > 8)
        {
            pos.y -= speed * Time.deltaTime;
        }
        transform.position = pos;
    }
}