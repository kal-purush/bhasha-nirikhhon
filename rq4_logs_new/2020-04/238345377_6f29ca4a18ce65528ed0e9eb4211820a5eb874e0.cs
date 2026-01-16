using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Cinemachine;

public class CamShake : MonoBehaviour
{
    float timer = 20;
    int shakerCounter = 0;
    private float ShakeElapsedTime = 0f;
    public bool Appeased = false;

    [SerializeField]
    public CinemachineVirtualCamera vc;
    public CinemachineBasicMultiChannelPerlin shakeIt;

    // Start is called before the first frame update
    void Start()
    {
        if (vc != null)
            shakeIt = vc.GetCinemachineComponent<Cinemachine.CinemachineBasicMultiChannelPerlin>();
    }

    // Update is called once per frame
    void Update()
    {
        if (!Appeased)
        {
            Shake();
        }
        else
        {
            shakeIt.m_AmplitudeGain = 0;
            shakeIt.m_FrequencyGain = 0;
            shakerCounter = 0;
            Appeased = false;
        }
    }
    void Shake()
    {
        if (timer >= 0)
            timer -= Time.deltaTime;
        else
        {
            shakerCounter++;
            shakeIt.m_AmplitudeGain = shakerCounter;
            shakeIt.m_FrequencyGain = shakerCounter;
            timer = 20;
        }
    }
}