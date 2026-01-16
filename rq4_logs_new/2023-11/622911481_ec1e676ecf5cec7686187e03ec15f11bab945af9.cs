// Code inspired from https://github.com/Lumidi/CameraShakeInCinemachine/blob/master/SimpleCameraShakeInCinemachine.cs

using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Cinemachine;

public class CameraShake : MonoBehaviour
{

    private float ShakeDuration = 0.3f;          // Time the Camera Shake effect will last
    private float ShakeAmplitude = 1.2f;         // Cinemachine Noise Profile Parameter
    private float ShakeFrequency = 2.0f;         // Cinemachine Noise Profile Parameter

    private float ShakeElapsedTime = 0f;

    // Cinemachine Shake
    private CinemachineVirtualCamera VirtualCamera;
    private CinemachineBasicMultiChannelPerlin virtualCameraNoise;

    // Use this for initialization
    void Start()
    {
        // Get Virtual Camera component
        this.VirtualCamera = GetComponent<CinemachineVirtualCamera>();
        // Get Virtual Camera Noise Profile
        this.virtualCameraNoise = VirtualCamera.GetCinemachineComponent<Cinemachine.CinemachineBasicMultiChannelPerlin>();
    }

    private void Update()
    {
        if (Input.GetKeyDown(KeyCode.Space))
        {
            ShakeCamera();
        }
    }

    public void ShakeCamera()
    {
        ShakeElapsedTime = ShakeDuration;
        StartCoroutine(ShakeCoroutine(ShakeAmplitude, ShakeFrequency));
    }

    public void ShakeCamera(float duration, float amplitude, float frequency)
    {
        ShakeElapsedTime = duration;
        StartCoroutine(ShakeCoroutine(amplitude, frequency));
    }

    private IEnumerator ShakeCoroutine(float shakeAmplitude, float shakeFrequency)
    {
        // If Camera Shake effect is still playing
        while (ShakeElapsedTime > 0)
        {
            // Set Cinemachine Camera Noise parameters
            virtualCameraNoise.m_AmplitudeGain = shakeAmplitude;
            virtualCameraNoise.m_FrequencyGain = shakeFrequency;

            // Update Shake Timer
            ShakeElapsedTime -= Time.deltaTime;
            yield return null;
        }
        // If Camera Shake effect is over, reset variables
        virtualCameraNoise.m_AmplitudeGain = 0f;
        ShakeElapsedTime = 0f;
        yield return null;
    }
}