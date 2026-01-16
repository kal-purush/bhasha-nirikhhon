using System.Collections;
using System.Collections.Generic;
using Unity.XR.CoreUtils;
using UnityEditor.Rendering.Universal;
using UnityEngine;
using UnityEngine.XR.Interaction.Toolkit;

public class YellowBall : Ball
{
    public override void LastBounceBehavior(Collision collision)
    {
        var contact = collision.contacts[0];
        var normal = contact.normal;
        Physics.gravity = -normal * 9.8f;
        var xrOrigin = FindObjectOfType<XROrigin>();
        xrOrigin.MoveCameraToWorldLocation(contact.point);
        xrOrigin.MatchOriginUp(normal);
    }
}