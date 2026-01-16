using System.Collections;
using System.Collections.Generic;
using UnityEngine;


/*
 * CheckpointChecker is an component of the "Sphere" object
 */
public class CheckpointChecker : MonoBehaviour
{
    //  theCar refers to the "PLayer" object
    public CarController theCar;

    //  OnTriggerEnter function is an in-built function that interacts with a Collider Obhject
    //  In this case, OnTriggerEnter function is automatically called when the "Player" enters a checkpoint
    //  which carries the Box Collider Component labelled with "Is Trigger"
    private void OnTriggerEnter(Collider other)
    {
        if (other.tag == "Checkpoint") 
        {
            //  other.GetComponent<Checkpoints>().cpNumber -> gives the
            //  specific checkpoint number of that checkpoint
            theCar.CheckpointHit(other.GetComponent<Checkpoints>().cpNumber); 
        }
    }
}