using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Cinemachine;
public class CameraTarget : MonoBehaviour
{
    CinemachineVirtualCamera virtualCamera;
    Collider2D col2d;
    int chamberIndex = 0;
    private void Awake()
    {
        col2d = GetComponent<Collider2D>();
        virtualCamera = GetComponent<CinemachineVirtualCamera>();
    }
    public void setIndex(int i)
    {
        chamberIndex = i;
    }
    private void OnTriggerEnter2D(Collider2D collision)
    {
        //BasicCharacter basicCharacter = collision.GetComponent<BasicCharacter>();
        if(collision.CompareTag("Player"))
        {
            CameraController.Instance.LowerAllCamerasPriority();
            virtualCamera.Priority = 11;
        }
    }
    private void OnTriggerExit2D(Collider2D collision)
    {
        if (collision.CompareTag("Player"))
        {
            bool updatedChamber = CameraController.Instance.updateChamber(chamberIndex);
            col2d.isTrigger = !updatedChamber;
            
        }
    }
}