using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Valve.VR.InteractionSystem;

//Keeps track of controllers and hands.
public class ControllerInput : MonoBehaviour {

    public GameObject controller1, controller2;
    public GameObject hmd;

    public SteamVR_Controller.Device device1, device2;
    public SteamVR_TrackedObject trackedObjRight = null, trackedObjLeft = null;

    public Hand hand1, hand2;
	
	// Update is called once per frame
	void Update () {
		if(controller1 == null)
        {
            controller1 = GameObject.Find("Hand1");
            //device1 = controller1.GetComponent<SteamVR_Controller>();
        }
        else
        {
            if(hand1 == null)
            {
                hand1 = controller1.GetComponent<Hand>();
            }
        }

        if(controller2 == null)
        {
            controller2 = GameObject.Find("Hand2");
        }
        else
        {
            if(hand2 == null)
            {
                hand2 = controller2.GetComponent<Hand>();
            }
        }

        if(hmd == null)
        {
            hmd = GameObject.Find("VRCamera (eye)");
        }
	}
}