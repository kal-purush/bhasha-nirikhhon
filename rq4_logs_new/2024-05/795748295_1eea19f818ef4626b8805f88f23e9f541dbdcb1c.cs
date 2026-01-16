using System.Collections;
using System.Collections.Generic;
using Cinemachine;
using UnityEngine;

public class CameraShake : MonoBehaviour
{
    public PlayerStats stats;
    public CinemachineVirtualCamera vcam;
    private float ShakeTime = 0.3f;
    private float Timer = 0;
    private float ShakeIntensity = 1.0f;
    private float ShakeFrequency = 2.0f;

    private void Start()
    {
        StopShake();
    }

    private void Update()
    {
        if (Timer > 0)
        {
            Timer -= Time.deltaTime;
        }
        else
        {
            StopShake();
        }
    }

    private void OnDisable()
    {
        stats.EventHealthDamaged -= StartShake;
    }

    private void OnEnable()
    {
        stats.EventHealthDamaged += StartShake;
    }

    private void StartShake(int newHealthValue)
    {
        CinemachineBasicMultiChannelPerlin _cbmcp = vcam.GetCinemachineComponent<CinemachineBasicMultiChannelPerlin>();
        _cbmcp.m_AmplitudeGain = ShakeIntensity;
        _cbmcp.m_FrequencyGain = ShakeFrequency;

        Timer = ShakeTime;
    }

    private void StopShake()
    {
        CinemachineBasicMultiChannelPerlin _cbmcp = vcam.GetCinemachineComponent<CinemachineBasicMultiChannelPerlin>();
        _cbmcp.m_AmplitudeGain = 0;
        _cbmcp.m_FrequencyGain = 0;

        Timer = 0;
    }

}