using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ResultCamera : MonoBehaviour {

    private Vector3 winCameraPos = new Vector3(0f, 2.5f, 1.5f);
    private Vector3 winCameraRot = new Vector3(-13f, 180f, 0f);
    private Vector3 loseCameraPos = new Vector3(0f, 4.5f, -2.5f);
    private Vector3 loseCameraRot = new Vector3(40f, 0f, 0f);

    public void cameraStart (int winner, ref GameObject camera1P, ref GameObject camera2P)
    {
        if(winner == 1)
        {
            camera1P.transform.position = winCameraPos;
            camera1P.transform.eulerAngles = winCameraRot;
            camera2P.transform.position = loseCameraPos;
            camera2P.transform.eulerAngles = loseCameraRot;
        }
        else if(winner == 2)
        {
            camera1P.transform.position = loseCameraPos;
            camera1P.transform.eulerAngles = loseCameraRot;
            camera2P.transform.position = winCameraPos;
            camera2P.transform.eulerAngles = winCameraRot;
        }
        else
        {
            camera1P.transform.position = winCameraPos;
            camera1P.transform.eulerAngles = winCameraRot;
            camera2P.transform.position = winCameraPos;
            camera2P.transform.eulerAngles = winCameraRot;
        }
    }

	public void cameraUpdate (int rotateSpeed, ref GameObject camera1P, ref GameObject camera2P)
    {
        camera1P.transform.RotateAround(Vector3.zero, Vector3.up, rotateSpeed * Time.deltaTime);
        camera2P.transform.RotateAround(Vector3.zero, Vector3.up, rotateSpeed * Time.deltaTime);
    }
}