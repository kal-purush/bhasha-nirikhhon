using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CamSwitch : MonoBehaviour
{
    public GameObject playerCam;
    public GameObject vrCam;

    public Canvas UI;

    

    // Update is called once per frame
    void Update()
    {
        if (Input.GetKeyDown(KeyCode.F1))
        {
            if (playerCam.activeInHierarchy)
            {
                playerCam.SetActive(false);
                vrCam.SetActive(true);
                UI.worldCamera = vrCam.GetComponent<Camera>();
            }
            else
            {
                playerCam.SetActive(true);
                vrCam.SetActive(false);
                UI.worldCamera = playerCam.GetComponent<Camera>();
            }
        }
    }
}