using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ViveControllerTest : MonoBehaviour {

    // 1
    private SteamVR_TrackedObject trackedObj;
    // 2
    private SteamVR_Controller.Device Controller
    {
        get { return SteamVR_Controller.Input((int)trackedObj.index); }
    }


    void Awake()
    {
        trackedObj = GetComponent<SteamVR_TrackedObject>();
    }

    void Update()
    {
        // 1  Refers to the touchpad
        if (Controller.GetAxis() != Vector2.zero)
        {
            Debug.Log(gameObject.name + Controller.GetAxis());
        }

        if (Controller.GetPressDown(SteamVR_Controller.ButtonMask.Touchpad))
        {
            Debug.Log(gameObject.name + " Touchpad Press");
        }

        if (Controller.GetPressUp(SteamVR_Controller.ButtonMask.Touchpad))
        {
            Debug.Log(gameObject.name + " Touchpad Release");
        }


        // 2 Refers to Hair Trigger
        if (Controller.GetHairTriggerDown())
        {
            Debug.Log(gameObject.name + " Trigger Press");
        }

        if (Controller.GetHairTriggerUp())
        {
            Debug.Log(gameObject.name + " Trigger Release");
        }


        // 3 Refers to Grip 
        if (Controller.GetPressDown(SteamVR_Controller.ButtonMask.Grip))
        {
            Debug.Log(gameObject.name + " Grip Press");
        }

        if (Controller.GetPressUp(SteamVR_Controller.ButtonMask.Grip))
        {
            Debug.Log(gameObject.name + " Grip Release");
        }


        //4 Refers to Application Menu (Button above Touchpad)
        if (Controller.GetPressDown(SteamVR_Controller.ButtonMask.ApplicationMenu))
        {
            Debug.Log(gameObject.name + "Application Menu");
        }

        //5 Refers to System (Button below Touchpad
        if (Controller.GetPressDown(SteamVR_Controller.ButtonMask.System))
        {
            Debug.Log(gameObject.name + "System");
        }
    }
}