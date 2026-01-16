using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Cinemachine;
using System;
using System.Text.RegularExpressions;

public class CameraController : MonoBehaviour
{
    public static event Action<int> OnPlayerTrigger;  
    public static event Action<int> OnScreen;       

    [SerializeField] private CinemachineVirtualCamera[] cameras; 

    [SerializeField] private int currentCameraIndex = 0; 

    private CinemachineVirtualCamera lastActiveCamera;  

    private void Start()
    {
        lastActiveCamera = cameras[currentCameraIndex];
    }

    private void OnTriggerEnter(Collider other)
    {
        
        if (other.CompareTag("Player"))
        {
           
            Vector3 direction = other.transform.position - transform.position;

            if (direction.x < 0) 
            {
                SwitchToNextCamera();
            }
            else if (direction.x > 0) 
            {
                SwitchToPreviousCamera();
            }
        }
    }

    private void SwitchToNextCamera()
    {
        currentCameraIndex = (currentCameraIndex + 1) % cameras.Length;
        SetCameraPriority(currentCameraIndex);
    }

    private void SwitchToPreviousCamera()
    {
        currentCameraIndex = (currentCameraIndex - 1 + cameras.Length) % cameras.Length;
        SetCameraPriority(currentCameraIndex);
    }

    private void SetCameraPriority(int index)
    {
      
        for (int i = 0; i < cameras.Length; i++)
        {
            cameras[i].Priority = (i == index) ? 1 : 0; 
        }


        CinemachineVirtualCamera activeCamera = GetActiveCamera();

        if (activeCamera != null && activeCamera != lastActiveCamera)
        {
           
            lastActiveCamera = activeCamera;

          
            string cameraName = activeCamera.name;


            Regex regex = new Regex(@"\d+");
            Match match = regex.Match(cameraName);

            if (match.Success){
                int cameraNumber = int.Parse(match.Value);
                Debug.Log("Cámara número: " + cameraNumber);
                OnScreen?.Invoke(cameraNumber);  
            }
        }
    }

    private CinemachineVirtualCamera GetActiveCamera()
    {
        return Array.Find(cameras, cam => cam.Priority == 1);
    }
}