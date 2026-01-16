using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Assets.Scripts.Enums;

namespace Hephaestus.CamaeraManagement
{
    public class PersonalCameraController : MonoBehaviour
    {
        public PlayerCameraEnum CameraChoice;
        ////private Camera selectedCamera;
        private Camera[] childCameras;
        private Camera firstPersonCamera;
        private Camera thirdPersonCamera;

        /// <summary>
        /// Initialises the camera controller variables.
        /// </summary>
        /// <param name="player"></param>
        public PersonalCameraController(GameObject player)
        {
            GetAllChildCameras(player);
            AssignNamedPublicCameras(childCameras);
        }

        /// <summary>
        /// Public Accessor for FirstPersonCamera
        /// </summary>
        public Camera FirstPersonCamera
        {
            get { return firstPersonCamera; }
            set { firstPersonCamera = value; }
        }

        /// <summary>
        /// Public Accessor for Third PersonCamera
        /// </summary>
        public Camera ThirdPersonCamera
        {
            get { return thirdPersonCamera; }
            set { thirdPersonCamera = value; }
        }

        public void SetPlayerPerspective(PlayerCameraEnum cameraChoice)
        {
            switch (cameraChoice)
            {
                case PlayerCameraEnum.FPSCamera:
                    if (firstPersonCamera != null)
                    {
                        Debug.Log("FirstPersonCamera Selected.");
                        deactivateAllCameras(childCameras);
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
                        deactivateAllCameras(childCameras);
                        thirdPersonCamera.enabled = true;
                    }
                    else
                    {
                        Debug.LogWarning("Selected ThirdPersonCamera, but no ThirdPersonCamera camera to assign to.");
                    }
                    break;

                default:
                    ///TODO: Set default if no selection
                    ///TODO: Log No Camera Set
                    break;
            }
        }

        /// <summary>
        /// Sets the Camera Manager's Child Cameras to the 
        /// child cameras of the object handed to it.
        /// </summary>
        /// <param name="parent"></param>
        private void GetAllChildCameras(GameObject parent)
        {
            childCameras = parent.transform.GetComponentsInChildren<Camera>();
        }

        /// <summary>
        /// Deactivates all cameras handed into class.
        /// </summary>
        /// <param name="cameras"></param>
        private void deactivateAllCameras(Camera[] cameras)
        {
            foreach (Camera camera in cameras)
            {
                if (camera != null)
                {
                    camera.enabled = false;
                }
            }
        }

        /// <summary>
        /// Assigns the named cameras from the cameras in array handed to it
        /// </summary>
        private void AssignNamedPublicCameras(Camera[] cameras)
        {
            foreach (Camera camera in cameras)
            {
                switch (camera.name)
                {
                    case "FirstPersonCamera":
                        FirstPersonCamera = camera;
                        break;
                    case "ThirdPersonCamera":
                        ThirdPersonCamera = camera;
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
    }
}