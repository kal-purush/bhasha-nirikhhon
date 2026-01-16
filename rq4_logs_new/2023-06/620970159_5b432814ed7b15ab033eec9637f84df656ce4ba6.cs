using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Cinemachine;

public class CheckCameraBounds : MonoBehaviour
{
    [SerializeField] CameraController cameraController;
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
    }

    public void OnTriggerEnter2D(Collider2D collision)
    {
        if(collision.gameObject.CompareTag("cameraB"))
        {
            Debug.Log("enteredd");
            // stop following player
            cameraController.cam.m_Follow = null;
        }
    }

    //private void OnTriggerExit2D(Collider2D collision)
    //{
    //    if (collision.gameObject.CompareTag("cameraBounds"))
    //    {
    //        // follow player again
    //        cameraController.cam.m_Follow = GameObject.FindGameObjectWithTag("Player").transform;
    //    }
    //}
}