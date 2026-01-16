using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AudioManager : MonoBehaviour
{

    [Header("Audio Source")] 
    [SerializeField] AudioSource musicSource;
    [SerializeField] AudioSource SFXSource;

    [Header("Audio Clip")] 
    public AudioClip teletubbies;
    public AudioClip barney;
    public AudioClip minions;
    public AudioClip bfart;
    public AudioClip tbfart;
    public AudioClip mfart;
    public AudioClip laugh;
    public AudioClip fart;

    public AudioClip currentClip; 

    private void Start()
    {
        musicSource.clip = currentClip;
        musicSource.Play();
    }

    public void PlaySFX(AudioClip clip)
    {
        SFXSource.PlayOneShot(clip);
    }
}