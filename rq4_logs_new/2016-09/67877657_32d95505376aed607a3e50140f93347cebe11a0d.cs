using UnityEngine;
using System.Collections;

public class GrabMemorySphere : MonoBehaviour {
    bool isGrabbing;
    Transform targetedSphere = null;
    Transform grabbedSphere = null;
    public SteamVR_TrackedObject trackedObject;
	void Start () {
        isGrabbing = false;
	}

    void Update()
    {
        var device = SteamVR_Controller.Input((int)trackedObject.index);
        if (device.GetTouchDown(SteamVR_Controller.ButtonMask.Trigger))
        {
            grabbedSphere = targetedSphere;
            grabbedSphere.GetComponent<MoveTowardsTarget>().doMove = false;
            isGrabbing = true;
        }
        else if (device.GetTouchUp(SteamVR_Controller.ButtonMask.Trigger))
        {
            grabbedSphere.GetComponent<MoveTowardsTarget>().doMove = true;
            grabbedSphere = null;
            isGrabbing = false;
        }
        if (grabbedSphere != null)
        {
            grabbedSphere.position = transform.position;
        }
    }
    void OnTriggerEnter(Collider collider)
    {
        if (isGrabbing == false)
        {
            targetedSphere = collider.transform;
        }
    }
    void OnTriggerExit(Collider collider)
    {
        if (isGrabbing == false)
        {
            if (targetedSphere == collider.transform)
            {
                targetedSphere = null;
            }
        }
    }
}