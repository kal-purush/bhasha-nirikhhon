using System;
using UnityEngine;

public class BGMManager: MonoBehaviour
{
    private AudioSource audioSource;
    public static BGMManager Instance { get; private set; }
    private void Awake()
    {
        Instance = this;
        audioSource = GetComponent<AudioSource>();
        DontDestroyOnLoad(this);
    }

    private void Start()
    {
        audioSource.outputAudioMixerGroup = AudioManager.Instance.AudioPropsSO.BGMAudioMixerGroup;
        PlayBGMMain();
    }

    public void PlayBGMMain()
    {
        audioSource.clip = AudioManager.Instance.AudioPropsSO.BGMMain;
        audioSource.Play();
    }

    public void PlayBGMCombat()
    {
        audioSource.clip = AudioManager.Instance.AudioPropsSO.BGMCombat;
        audioSource.Play();
    }
    
    
}