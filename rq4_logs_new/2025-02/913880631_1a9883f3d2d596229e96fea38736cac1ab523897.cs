using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class LowPickUpItem : MonoBehaviour
{
    public Camera mainCam;
    public LayerMask gunLayerMask;
    public LayerMask keyLayerMask;
    public GameObject gunObj;
    public GameObject fKey;

    public bool hasKey;
    private void Start()
    {
        gunObj.SetActive(false);
        fKey.SetActive(false);
        hasKey = false;
    }

    private void Update()
    {
        PickUpGun();
        PickUpKey();
    }

    private void PickUpGun()
    {
        if (Physics.Raycast(mainCam.transform.position, mainCam.transform.forward, out RaycastHit hit, 2, gunLayerMask))
        {
            Debug.Log("Gun detected: " + hit.transform.name + " on layer " + hit.transform.gameObject.layer); // Kiểm tra layer
            fKey.SetActive(true);
            if (Input.GetKeyDown(KeyCode.F))
            {
                gunObj.SetActive(true);
                Destroy(hit.transform.gameObject);
            }
        }
        else
        {
            fKey.SetActive(false);
        }
    }

    private void PickUpKey()
    {
        if (Physics.Raycast(mainCam.transform.position, mainCam.transform.forward, out RaycastHit hit, 2, keyLayerMask))
        {
            fKey.SetActive(true);
            if (Input.GetKeyDown(KeyCode.F))
            {
                Destroy(hit.transform.gameObject);
                hasKey = true;
            }
        }
        else
        {
            fKey.SetActive(false);
        }
    }
}