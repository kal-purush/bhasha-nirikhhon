using UnityEngine;
using System.Collections;

public class CameraRaycast : MonoBehaviour 
{
    Camera myCamera;

    public GameObject toHide;
    public bool turnInvisible = false;

    void Start()
    {
        myCamera = GetComponent<Camera>();     
    }

    void Update()
    {
        Ray ray = myCamera.ViewportPointToRay(new Vector3(0.5F, 0.5F, 0));
        RaycastHit hit;

       // float myAlpha = toHide.GetComponent<MeshRenderer>().material.color.a;

        if (Physics.Raycast(ray, out hit))
        { 
            // make this object transparent if its tagged as 'ground'
            if (hit.transform.tag == "ground")
            {
                toHide = hit.transform.gameObject;
                toHide.GetComponent<MeshRenderer>().enabled = false;
                //myAlpha--;
                turnInvisible = true;
            }

            // if player becomes visible once again, turn the invisble object back on
            if (toHide.GetComponent<MeshRenderer>().enabled == false && hit.transform.tag == "Player")
            {
                toHide.GetComponent<MeshRenderer>().enabled = true;
            }
        }
    }
}