using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.EventSystems;

public class RayManager : MonoBehaviour
{
    public OVRCameraRig cameraRig;
    public OVRInputModule inputModule;

    // Use this for initialization
    void Start()
    {

    }

    // Update is called once per frame
    void Update()
    {
        OVRInput.Controller activeController = OVRInput.GetActiveController();
        Transform activeTransform = cameraRig.centerEyeAnchor;

        if ((activeController == OVRInput.Controller.LTouch) || (activeController == OVRInput.Controller.LTrackedRemote))
            activeTransform = cameraRig.leftHandAnchor;

        if ((activeController == OVRInput.Controller.RTouch) || (activeController == OVRInput.Controller.RTrackedRemote))
            activeTransform = cameraRig.rightHandAnchor;

        if (activeController == OVRInput.Controller.Touch)
            activeTransform = cameraRig.rightHandAnchor;

        OVRGazePointer.instance.rayTransform = activeTransform;
        inputModule.rayTransform = activeTransform;
    }
}