using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class UpdateClippingPlane : MonoBehaviour
{
    public Camera cam;

    // Start is called before the first frame update
    void Start()
    {
        cam.farClipPlane = cam.nearClipPlane + .001f;
    }

}