using System.Collections.Generic;
using UnityEngine;
using UnityEngine.XR;
using UnityEngine.XR.Interaction.Toolkit;

public class HandAnimator : MonoBehaviour
{
    public float speed = 0.5f;
    public XRController controller = null;

    public Animator animator = null;

    private readonly List<Finger> gripfingers = new List<Finger>()
    {
        new Finger(FingerType.Middle),
        new Finger(FingerType.Ring),
        new Finger(FingerType.Pinky)
    };

    private readonly List<Finger> pointFingers = new List<Finger>()
    {
        new Finger(FingerType.Index),
        new Finger(FingerType.Thumb)
    };


    // [SerialzedField] private 
    
    private void Awake()
    {
        animator = GetComponent<Animator>();
    }

    private void Update()
    {
        // Store input

        // Smooth input values

        // Apply smoothed values
    }

    private void CheckGrip()
    {
        // if(controller.inputDevice.);
    }

    private void CheckPointer()
    {

    }

    private void SetFingerTargets(List<Finger> fingers, float value)
    {

    }

    private void SmoothFinger(List<Finger> fingers)
    {

    }

    private void AnimateFinger(List<Finger> fingers)
    {

    }

    private void AnimateFinger(string finger, float blend)
    {

    }
}