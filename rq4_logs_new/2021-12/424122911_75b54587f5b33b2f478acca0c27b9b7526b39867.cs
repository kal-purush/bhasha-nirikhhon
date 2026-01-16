using System;
using Cinemachine;
using UnityEngine;

namespace Code.Scripts
{
    public class CameraInstance : MonoBehaviour
    {
        public string cameraName;
        
        public CinemachineVirtualCamera virtualCam;
        public int originalPriority;

        private void Start()
        {
            if (cameraName == "")
            {
                cameraName = gameObject.name;
            }
            virtualCam = GetComponent<CinemachineVirtualCamera>();
            virtualCam.Priority = originalPriority;

            var existingCam = CameraManager.Instance.cameraInstances.Find(cam => cam.cameraName == cameraName);
            if (existingCam == null)
            {
                CameraManager.Instance.cameraInstances.Add(this);
            }
        }
    }
}