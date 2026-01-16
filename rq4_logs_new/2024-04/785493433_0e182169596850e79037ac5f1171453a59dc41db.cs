using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ParallaxBackground: MonoBehaviour
{
    public float parallaxFactor = 0.5f; // ??????????????????

    private Transform cameraTransform;
    private Vector3 lastCameraPosition;

    void Start()
    {
        cameraTransform = Camera.main.transform;
        lastCameraPosition = cameraTransform.position;
    }

    void Update()
    {
        // ?????????
        float deltaX = cameraTransform.position.x - lastCameraPosition.x;
        float deltaY = cameraTransform.position.y - lastCameraPosition.y;

        // ??????????????
        transform.position += new Vector3(deltaX * parallaxFactor, deltaY * parallaxFactor, 0);

        // ???????
        lastCameraPosition = cameraTransform.position;
    }
}