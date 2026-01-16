using System.Collections;
using System.Collections.Generic;
using UnityEngine;

	/*
	 * This class is for grabbing and releasing Objects using a box collider 
	 * to prevent the controller from collisions
	 */

public class ControllerGrabObject : MonoBehaviour {
	
	private Valve.VR.EVRButtonId triggerButton = Valve.VR.EVRButtonId.k_EButton_SteamVR_Trigger;
	private Valve.VR.EVRButtonId gripButton = Valve.VR.EVRButtonId.k_EButton_Grip;

	private SteamVR_Controller.Device controller{ 
		get { return SteamVR_Controller.Input((int)trackedObj.index);}
	}

	private SteamVR_TrackedObject trackedObj;

	private GameObject collidingObject, objectInHand; 

	void Update () {
		trackedObj = GetComponent<SteamVR_TrackedObject>();

		if (controller.GetPressDown (gripButton) && objectInHand != null) {
			objectInHand.transform.parent = this.transform;
		}
		if (controller.GetPressUp (gripButton) && objectInHand != null) {
			objectInHand.transform.parent = null;
		}

		if (controller.GetHairTriggerDown())
		{
			if (collidingObject)
			{
				GrabObject();
			}
		}
			
		if (controller.GetHairTriggerUp())
		{
			if (objectInHand)
			{
				ReleaseObject();
			}
		}
	}

	private void SetCollidingObject(Collider collider)
	{
		if (collidingObject || !collider.GetComponent<Rigidbody>())
		{
			return;
		}
		collidingObject = collider.gameObject;
	}

	public void OnTriggerEnter(Collider other)
	{
		SetCollidingObject(other);
	}
		
	public void OnTriggerStay(Collider other)
	{
		SetCollidingObject(other);
	}

	public void OnTriggerExit(Collider other)
	{
		if (!collidingObject)
		{
			return;
		}

		collidingObject = null;
	}

	private void GrabObject()
	{
		objectInHand = collidingObject;
		collidingObject = null;

		var objContrJoint = AddFixedJoint();
		objContrJoint.connectedBody = objectInHand.GetComponent<Rigidbody>();
	}

	private FixedJoint AddFixedJoint()
	{
		FixedJoint fx = gameObject.AddComponent<FixedJoint>();
		fx.breakForce = 20000;
		fx.breakTorque = 20000;
		return fx;
	}

	private void ReleaseObject()
	{
		if (GetComponent<FixedJoint>())
		{
			GetComponent<FixedJoint>().connectedBody = null;
			Destroy(GetComponent<FixedJoint>());

			objectInHand.GetComponent<Rigidbody>().velocity = controller.velocity;
			objectInHand.GetComponent<Rigidbody>().angularVelocity = controller.angularVelocity;
		}
		objectInHand = null;
	}
}