using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.InputSystem;
using UnityEngine.XR.Interaction.Toolkit;


public class glowStickController : XRGrabInteractable
{

    public bool beingHeld;

    protected override void OnSelectEntered(SelectEnterEventArgs args)
    {
        beingHeld = true;
        base.OnSelectEntered(args);
    }

    protected override void OnSelectExited(SelectExitEventArgs args)
    {
        beingHeld = false;
        base.OnSelectExited(args);
    }
    protected override void OnHoverEntered(HoverEnterEventArgs args)
    {
        if(beingHeld == true)
        {
           LightSwitcher();
        }
        base.OnHoverEntered(args);
    }

    void LightSwitcher()
    {

            GetComponent<Light>().enabled = true;
        
    }
}