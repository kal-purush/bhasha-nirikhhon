using System.Collections;
using System.Collections.Generic;
using UnityEngine;

//Hoe item script.
public class HoeItem : ToolItem {

    //Minimum blade velocity to be valid.
    public float maxVelocity = 1;

    private void Update()
    {
        if(GetComponent<InteractionPickup>() != null)
        {
            if (GetComponent<InteractionPickup>().isHeld)
            {
                isValid = isToolValid();
            }
        }
       
    }

    //Write new isToolValid function
    public override bool isToolValid()
    {
        //Check for y-velocity of the blade.
        Transform blade = transform.GetChild(0);
        float velocity = blade.GetComponent<HoeLinker>().velocity;
        
        if(velocity <= maxVelocity)
        {
            return true;
        }
        else
        {
            return false;
        }

    }

}