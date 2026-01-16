using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Valve.VR.InteractionSystem;

public class CircularDriveCustom : CircularDrive {

	public override void HandHoverUpdate(Hand hand)
    {
        if (hand.GetStandardInteractionButtonDown())
        {
            // Trigger was just pressed
            lastHandProjected = ComputeToTransformProjected(hand.hoverSphereTransform);

            if (hoverLock)
            {
                hand.HoverLock(GetComponent<Interactable>());
                handHoverLocked = hand;
            }

            driving = true;

            ComputeAngle(hand);
            UpdateAll();

            ControllerButtonHints.HideButtonHint(hand, Valve.VR.EVRButtonId.k_EButton_SteamVR_Trigger);
        }
        else if (hand.GetStandardInteractionButtonUp())
        {
            // Trigger was just released
            if (hoverLock)
            {
                hand.HoverUnlock(GetComponent<Interactable>());
                handHoverLocked = null;
            }
        }
        else if (driving && hand.GetStandardInteractionButton() && hand.hoveringInteractable == GetComponent<Interactable>())
        {
            ComputeAngle(hand);
            UpdateAll();
        }
    }
}