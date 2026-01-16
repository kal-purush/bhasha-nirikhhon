using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CameraFollowsMC : MonoBehaviour
{


    [SerializeField] private Vector3 offset;
    [SerializeField] private float damping;

    public Transform target;

    private Vector3 vel = Vector3.zero;
    // Start is called before the first frame update

    // Update is called once per frame
    private void FixedUpdate()
    {
        Vector3 desiredPosition = target.position + offset;
        desiredPosition.z = transform.position.z;

        transform.position = Vector3.SmoothDamp(transform.position, desiredPosition,
            ref vel, damping);

    }
}