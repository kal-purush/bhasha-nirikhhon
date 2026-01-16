using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CameraMovement : MonoBehaviour
{
    private int current_num;
    private float x;
    private float y;
    private float z;
    private Camera cam;
    private Vector3 transformvalue;
    private Vector3 extransformvalue;

    public float speed = 2f;
   

    IEnumerator stay10Seconds()
    {
        yield return new WaitForSeconds(10);
    }
    private void FixedUpdate()
    {   

        SceneManager scenes_ = GameObject.Find("Scenes").GetComponent<SceneManager>();
        current_num = scenes_.current;

        if(current_num==2)
        {
            x = (float)1.368;
            y = (float)1.091;
            z = (float)26.55;
            transformvalue = new Vector3(x,y,z);
            Camera.main.transform.position = new Vector3(x,y,z);
        }

        if(current_num==3)
        {  
            extransformvalue = new Vector3(x,(float)0.80,(float)27.15);
         //   cam = GameObject.Find("Crowd_Cam").GetComponent<Camera>();
            transform.position = Vector3.MoveTowards(transform.position,extransformvalue,Time.deltaTime*speed);
        }

        if(current_num ==5)
        { 
            stay10Seconds();
            // extransformvalue = new Vector3((float)4.77,(float)1.11,(float)6.13);
            // cam = GameObject.Find("Crowd_Cam").GetComponent<Camera>();
            // transform.position = Vector3.MoveTowards(transform.position,extransformvalue,Time.deltaTime*speed);
        }
        if(current_num == 0)
        {
            if(extransformvalue!= transformvalue)
            {
               extransformvalue = new Vector3((float)4.77,(float)1.11,(float)6.13);
           //   cam = GameObject.Find("Crowd_Cam").GetComponent<Camera>();
              transform.position = Vector3.MoveTowards(transform.position,extransformvalue,Time.deltaTime*speed);
            }
        }
    }
}