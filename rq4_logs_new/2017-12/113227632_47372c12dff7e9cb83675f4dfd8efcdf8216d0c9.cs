using UnityEngine;
using Assets.Scripts.Enums;
using System;
using System.Collections.Generic;

[RequireComponent(typeof(Rigidbody))]
public class RigidBodyPlayerController : MonoBehaviour {

    #region Fields
    public PlayerCameraEnum CameraChoice;
    private Rigidbody rb;
    ////private Camera selectedCamera;
    private Camera[] childCameras;
    private Camera firstPersonCamera;
    private Camera thirdPersonCamera;
    ////private Camera externalCamera;
    #endregion

    /// <summary>
    /// Player Control Set Up
    /// </summary>
    public void Start () {
        InitaliseFields();
        SetPlayerCamera();
	}

    /// <summary>
    /// Called once a frame
    /// </summary>
    public void Update()
    {

    }

    /// <summary>
    /// Sets the camera that the player is using based on an input Enum
    /// </summary>
    private void SetPlayerCamera()
    {
        switch (CameraChoice)
        {
            case PlayerCameraEnum.FPSCamera:
                if (firstPersonCamera != null)
                {
                    Debug.Log("FirstPersonCamera Selected.");
                    deactivateAllCameras();
                    firstPersonCamera.enabled = true;
                }
                else
                {
                    Debug.LogWarning("Selected FirstPersonCamera, but no FirstPersonCamera to assign to.");
                }
                break;

            case PlayerCameraEnum.ThirdPersonCamera:
                if (firstPersonCamera != null)
                {
                    Debug.Log("ThirdPersonCamera Selected.");
                    deactivateAllCameras();
                    thirdPersonCamera.enabled = true;
                }
                else
                {
                    Debug.LogWarning("Selected ThirdPersonCamera, but no ThirdPersonCamera camera to assign to.");
                }
                break;

            //case PlayerCameraEnum.ExternalCamera:
            //    if (firstPersonCamera != null)
            //    {
            //        Debug.Log("ExternalCamera Selected.");
            //        externalCamera.enabled = true;
            //    }
            //    else
            //    {
            //        Debug.LogWarning("Selected ExternalCamera, but no ExternalCamera to assign to.");
            //    }
            //    break;

            default:
                ///TODO: Set default if no selection
                ///TODO: Log No Camera Set
                break;
        }
    }

    /// <summary>
    /// Gets all Cameras from parent,
    /// Sets rigidBody
    /// </summary>
    private void InitaliseFields()
    {
        rb = GetComponentInChildren<Rigidbody>();
        childCameras = transform.GetComponentsInChildren<Camera>();
        foreach (Camera camera in childCameras)
        {
            switch (camera.name)
            {
                case "FirstPersonCamera":
                    firstPersonCamera = camera;
                    break;
                case "ThirdPersonCamera":
                    thirdPersonCamera = camera;
                    break;
                //case "ExternalCamera":
                    //externalCamera = camera;
                    //break;
                default:
                    Debug.Log("Can't assign camera, no enumerator or init check for name.");
                    break;
            }
        }
    }

    private void deactivateAllCameras()
    {
        foreach (Camera camera in childCameras)
        {
            camera.enabled = false;
        }
    }
}