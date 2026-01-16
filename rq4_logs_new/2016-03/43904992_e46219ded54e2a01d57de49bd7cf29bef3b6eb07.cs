using UnityEngine;
using System.Collections.Generic;

public class HandPositionSelector : MonoBehaviour
{
    public Vector3 positionWhenHooked;
    public Vector3 positionWhenMoving;
    private HookshotControl hookshotControl;
    private LateralMovement movement;

    // Use this for initialization
    void Start()
    {
        hookshotControl = GetComponentInChildren<HookshotControl>();
        movement = transform.root.GetComponent<LateralMovement>();
    }

    // Update is called once per frame
    void Update()
    {
        if ((hookshotControl.State != HookshotControl.HookshotState.HOOKED && movement.isGrounded()) || 
            hookshotControl.State == HookshotControl.HookshotState.RETRACTING)
        {
            transform.localPosition = positionWhenMoving;
        }
        else
        {
            transform.localPosition = positionWhenHooked;
        }
    }
}