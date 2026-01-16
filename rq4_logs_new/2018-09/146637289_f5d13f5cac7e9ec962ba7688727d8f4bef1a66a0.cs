using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class VectorMovement : MonoBehaviour
{
    Vector3 pos;
    Vector3 vel;
    Vector3 clickPos;
    Vector3 translatedClick;

    public Camera cam;

    float speedMult = 3f;
    void Start()
    {
        pos = this.transform.position;
        //vel = new Vector3(-0.1f, 0, 0);
    }

    void FixedUpdate()
    {
        MouseMove();
    }

    void KeyMove(){
        //if(Input.GetKey(KeyCode.LeftArrow)){
        //    pos += vel;
        //}
        vel = new Vector3(Input.GetAxis("Horizontal"), Input.GetAxis("Vertical"), 0); //* speedMult;
        this.GetComponent<Rigidbody>().velocity = vel;
        //pos = pos + vel;
        //this.transform.position = pos;
    }
    void MouseMove(){
        if(Input.GetMouseButton(0)){
            //Debug.Log("Click");
            Debug.Log("clickPos: " + clickPos);
            clickPos = Input.mousePosition;
            translatedClick = cam.ScreenToWorldPoint(new Vector3(clickPos.x, clickPos.y, cam.nearClipPlane));
            Debug.Log("translatedClick: " + translatedClick);
            Debug.DrawRay(Vector3.zero, translatedClick, Color.cyan);

            this.GetComponent<Rigidbody>().AddForce(new Vector3(translatedClick.x, translatedClick.y * speedMult, 0f));
        }
    }
}