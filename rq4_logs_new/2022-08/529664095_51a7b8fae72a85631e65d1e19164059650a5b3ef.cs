// Script coded by Ben Baeyens - https://www.benbaeyens.com/
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AudioManager : MonoBehaviour
{
    #region variables

    public static AudioManager Instance
    {
        get; private set;
    }

    private AudioSource source;

    #endregion


    #region methods

    private void Awake()
    {
        if (Instance == null)
        {
            Instance = this;
            DontDestroyOnLoad(gameObject);
        }
        else
        {
            Destroy(gameObject);
        }

        source = GetComponent<AudioSource>();
    }

    public void Play(AudioClip clip, float volume = 1f)
    {
        if (clip != null)
        {
            source.PlayOneShot(clip, volume);
        }
        else
        {
            Debug.LogWarning("Audio Manager: Passed through clip was NULL");
        }
    }


    #endregion
}