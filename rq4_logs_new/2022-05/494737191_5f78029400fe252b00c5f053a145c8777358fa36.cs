using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Cameracollider : MonoBehaviour
{
    public Camera leftcamera;
    public Camera middlecamera;
    public Camera rightcamera;
    public Camera maincamera;

    int[] count = {1};

    private void Update()
    {
        if (count[0] == 1)
        {
            if (Input.GetKeyDown("r"))
            {
                Debug.Log("11" + count[0]);
                maincamera.enabled = false;
                leftcamera.enabled = true;

            }
            if (Input.GetKeyUp("r"))
            {
                Debug.Log("22" + count[0]);
                maincamera.enabled = true;
                leftcamera.enabled = false;
                count[0] = count[0] + 1;
            }

        }
        else if (count[0] == 2)
        {
            if (Input.GetKeyDown("r"))
            {
                Debug.Log("11" + count[0]);
                maincamera.enabled = false;
                middlecamera.enabled = true;

            }
            if (Input.GetKeyUp("r"))
            {
                Debug.Log("22" + count[0]);
                maincamera.enabled = true;
                middlecamera.enabled = false;
                count[0] = count[0] + 1;
            }

            Debug.Log("2222");
        }
        else if (count[0] == 3)
        {
            if (Input.GetKeyDown("r"))
            {
                Debug.Log("11" + count[0]);
                maincamera.enabled = false;
                rightcamera.enabled = true;

            }
            if (Input.GetKeyUp("r"))
            {
                Debug.Log("22" + count[0]);
                maincamera.enabled = true;
                rightcamera.enabled = false;
                count[0] = count[0] - 2;
            }
            Debug.Log("3333");
        }
    }
}








