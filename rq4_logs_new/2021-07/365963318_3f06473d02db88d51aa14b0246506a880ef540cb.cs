using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CameraController : MonoBehaviour
{
    public Transform targetPlayer;
    private float smoothspeed = 10f;
    [SerializeField]private Vector3 offset;

    private void LateUpdate()
    {
        Vector3 desiredPosition = targetPlayer.position + offset;
        Vector3 smoothedPosition = Vector3.Lerp(transform.position, desiredPosition, smoothspeed * Time.deltaTime);
        transform.position = smoothedPosition;
    }
}