using System.Collections;
using System.Collections.Generic;
using System.Numerics;
using UnityEngine;
using Vector3 = UnityEngine.Vector3;

public class CameraSwitch : MonoBehaviour
{
    public GameObject mainCam;
    public GameObject whiteKingCam;
    public GameObject blackKingCam;

    // Start is called before the first frame update
    public void CreateCameras()
    {
        whiteKingCam = GetChildWithName(GameObject.FindGameObjectsWithTag("King")[0], "Camera");
        blackKingCam = GameObject.FindGameObjectsWithTag("King")[1].transform.Find("Camera").gameObject;

        ChangePosition(blackKingCam.transform, new Vector3(0f, 0f, 0.4f));
        blackKingCam.transform.Rotate(60f, 180f, 0f); 

        whiteKingCam.SetActive(false);
        blackKingCam.SetActive(false);

    }

    // Update is called once per frame
    void Update()
    {
        if (Input.GetButtonDown("Camera1"))
        {
            if(mainCam.activeSelf == true)
            {
                mainCam.SetActive(false);
                whiteKingCam.SetActive(true);
                blackKingCam.SetActive(false);
            }
            else
            {
                mainCam.SetActive(true);
                whiteKingCam.SetActive(false);
                blackKingCam.SetActive(false);
            }
        }
        if (Input.GetButtonDown("Camera2"))
        {
            if (mainCam.activeSelf == true)
            {
                mainCam.SetActive(false);
                whiteKingCam.SetActive(false);
                blackKingCam.SetActive(true);
            }
            else
            {
                mainCam.SetActive(true);
                whiteKingCam.SetActive(false);
                blackKingCam.SetActive(false);
            }
        }
    }


    GameObject GetChildWithName(GameObject obj, string name)
    {
        Transform trans = obj.transform;
        Transform childTrans = trans.Find(name);
        if (childTrans != null)
        {
            return childTrans.gameObject;
        }
        else
        {
            return null;
        }
    }

    void ChangePosition(Transform transform, Vector3 offset)
    {
        transform.position = new Vector3(transform.position.x + offset.x, transform.position.y + offset.y, transform.position.z + offset.z);
    }
}