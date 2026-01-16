using System.Collections;
using System.Collections.Generic;
using Unity.XR.CoreUtils;
using UnityEngine;

public class CharacterMove : MonoBehaviour
{
    public static XROrigin xrOrigin;

    void Start()
    {
        xrOrigin = FindObjectOfType<XROrigin>();
    }

    public static void Teleport(Vector3 position, Vector3 normal, bool changeGravity = true)
    {
        if (changeGravity)
        {
            Physics.gravity = -normal * 9.8f;
        }

        var vector = Vector3.Normalize(xrOrigin.transform.position - position);
        var forward = Vector3.Cross(normal, vector);

        //xrOrigin.MoveCameraToWorldLocation(contact.point + normal * 1.36144f);
        xrOrigin.transform.position = position;

        xrOrigin.MatchOriginUpCameraForward(normal, forward);
        xrOrigin.MatchOriginUpOriginForward(normal, forward);
    }
}