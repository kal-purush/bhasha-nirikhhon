using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.InputSystem;

public class GrappleGun : MonoBehaviour
{
    public GameObject ropePrefab;
    public GameObject segmentPrefab;
    public float segmentDistance = .5f;
    public Transform launchPoint;
    public float force = 100f;
    public float segmentCutoff = .1f;

    private GameObject ropeInstance;

    public void OnGrapple(InputAction.CallbackContext ctx)
    {
        if (ctx.performed)
        {
            ropeInstance = Instantiate(ropePrefab, launchPoint.position, ropePrefab.transform.rotation);
        }
    }

    private void FixedUpdate()
    {
        if (ropeInstance != null)
        {
            ropeInstance.GetComponent<Rigidbody>().AddForce(transform.forward * force * Time.deltaTime);
        }
    }

    private void Update()
    {
        if (ropeInstance != null)
        {
            if (Vector3.Distance(launchPoint.position, ropeInstance.transform.position) > segmentCutoff)
            {
                GameObject segmentInstance = Instantiate(segmentPrefab, ropeInstance.transform.position, segmentPrefab.transform.rotation, ropeInstance.transform);
                segmentInstance.transform.position = new Vector3(transform.position.x, transform.position.y + .5f, transform.position.z);
            }
        }
    }
}