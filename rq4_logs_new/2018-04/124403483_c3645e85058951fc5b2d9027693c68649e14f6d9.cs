using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class FollowPlayer : MonoBehaviour {

    // Public variables
    public Transform playerTarget;
    public float smoothing = 5f;

    // Private variables
    private Vector3 offset;

    void Start()
    {
        offset = transform.position - playerTarget.position;
    }

    void FixedUpdate()
    {
        Vector3 targetPosition = playerTarget.position + offset;
        transform.position = Vector3.Lerp(transform.position, targetPosition, smoothing * Time.deltaTime);
    }

}