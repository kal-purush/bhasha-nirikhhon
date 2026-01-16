using UnityEngine;
using OmnicatLabs.Tween;
using Cinemachine;

public class CameraEffects : MonoBehaviour
{
    public float FOVTransitionTime = .25f;
    public float TiltTransitionTime = 1.2f;

    private Camera cam;
    private CinemachineVirtualCamera vCam;

    private void Start()
    {
        cam = GetComponentInChildren<Camera>();
        vCam = GetComponentInChildren<CinemachineVirtualCamera>();
    }

    public void AdjustFOV(float newFOV)
    {
        vCam.TweenFOV(newFOV, FOVTransitionTime);
    }

    public void AdjustTilt(float zTilt)
    {
        vCam.TweenDutch(zTilt, TiltTransitionTime, null, EasingFunctions.Ease.EaseOutCubic);
    }
}