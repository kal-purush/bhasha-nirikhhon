using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AudioPlayer : MonoBehaviour
{

    static public AudioPlayer Instance = null;
    public AudioSource audioSource = null;

    private void Awake()
    {
        var instances = FindObjectsOfType<AudioPlayer>();
        if (instances.Length > 1)
        {
            Destroy(gameObject);
        }
        else
        {
            Instance = this;
            audioSource = GetComponent<AudioSource>();
            DontDestroyOnLoad(gameObject);
        }
    }

    public void PlayAudioClip(AudioClip vo)
    {
        if (audioSource)
        {
            audioSource.PlayOneShot(vo);
        }
    }
}