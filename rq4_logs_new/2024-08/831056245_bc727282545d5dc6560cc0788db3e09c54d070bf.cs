using System.Collections;
using System.Collections.Generic;
using UnityEngine;

using UltimateXR.Core;
using UltimateXR.Manipulation;
using UltimateXR.Devices;
using UltimateXR.Haptics;
using UltimateXR.Avatar;

public class PlayerController : MonoBehaviour
{
    private void OnEnable() {
        UxrGrabManager.Instance.ObjectGrabbed += TriggerHaptic;
    }

    private void OnDisable() {
        UxrGrabManager.Instance.ObjectGrabbed -= TriggerHaptic;
    }

    // When a player grabs an object, it sends a haptic feedback
    private void TriggerHaptic(object sender, UxrManipulationEventArgs e) {
        UxrAvatar.LocalAvatar.ControllerInput.SendGrabbableHapticFeedback(e.GrabbableObject, UxrHapticClipType.Click);
    }
}