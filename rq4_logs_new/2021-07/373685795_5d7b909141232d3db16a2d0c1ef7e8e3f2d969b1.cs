using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DoingSomeE : MonoBehaviour
{
    public LayerMask layerizor;
    public Transform camSpot;


    HandScannerTouchPad currentScanner;

    // Update is called once per frame
    void Update()
    {
        RaycastHit boop;

        if(Physics.Raycast(camSpot.position, camSpot.forward, out boop, 2f, layerizor, QueryTriggerInteraction.Collide))
        {
            if(boop.collider.CompareTag("ScannieBoi") && Input.GetKey(KeyCode.E))
            {
                currentScanner = boop.collider.gameObject.GetComponent<HandScannerTouchPad>();
                currentScanner.DoTheEnterThingFPS();
            }
            else
            {
                if (currentScanner != null)
                {
                    currentScanner.DoTheExitThingFPS();
                    currentScanner = null;
                }
            }
        }
    }
}