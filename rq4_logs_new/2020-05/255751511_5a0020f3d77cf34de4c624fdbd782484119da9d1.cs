using UnityEngine;
using System.Collections;
 
public class CameraMovement : MonoBehaviour 
{     
    void Update ()
    {
        Vector3 p = GetBaseInput();
        transform.Translate(p);
    }
     
    private Vector3 GetBaseInput() //returns the basic values, if it's 0 than it's not active.
    {
        Vector3 p_Velocity = new Vector3();
        if (Input.GetKey (KeyCode.W)){
            p_Velocity += new Vector3(0, 0.1f, 0);
        }
        if (Input.GetKey (KeyCode.S)){
            p_Velocity += new Vector3(0, -0.1f, 0);
        }
        if (Input.GetKey (KeyCode.A)){
            p_Velocity += new Vector3(-0.1f, 0, 0);
        }
        if (Input.GetKey (KeyCode.D)){
            p_Velocity += new Vector3(0.1f, 0, 0);
        }
        return p_Velocity;
    }
}