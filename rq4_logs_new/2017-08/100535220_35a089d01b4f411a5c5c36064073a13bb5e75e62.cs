using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Valve.VR.InteractionSystem;

//Script that every item that can be stored and used will come from.
[RequireComponent(typeof(BaseItem))]
public class InteractionItem : InteractionPickup {

    public bool debug = false;

    private void Update()
    {

        if (debug)
        {
            //Apply the offset onto the player
            Vector3 offset = GetComponent<BaseItem>().offset;
            transform.localPosition = offset;

            //Apply rotation offset
            Vector3 rotOffset = GetComponent<BaseItem>().rotOffset;
            transform.localEulerAngles = rotOffset;

        }

    }

    //When we attach it to the hand
    public override void OnAttachedToHand(Hand hand)
    {
        Debug.Log("Attaching...");

        //Apply the offset onto the player
        Vector3 offset = GetComponent<BaseItem>().offset;
        transform.localPosition = offset;

        //Apply rotation offset
        Vector3 rotOffset = GetComponent<BaseItem>().rotOffset;
        transform.localEulerAngles = rotOffset;

        //Disable movement in all directions
        GetComponent<Rigidbody>().isKinematic = true;

    }
}