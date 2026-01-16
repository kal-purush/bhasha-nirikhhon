using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AudioControl : MonoBehaviour
{
    private AudioSource audioSource;
    public AudioClip audioClip;


    void Start()
    {
        audioSource = GetComponent<AudioSource>();
    }

    void Update()
    {
    }

    public void OnSound()
    {
        audioSource.PlayOneShot(audioClip, 2.0f);
    }
}