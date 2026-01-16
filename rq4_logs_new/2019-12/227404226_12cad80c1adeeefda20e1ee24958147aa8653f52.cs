using UnityEngine;
using System.Collections;

public class FollowCameraText : MonoBehaviour
{
    public Camera CameraToFollow;
    public float DistanceFromCamera;

    void Update()
    {
        transform.position = CameraToFollow.transform.position + CameraToFollow.transform.forward * DistanceFromCamera;
    }
}