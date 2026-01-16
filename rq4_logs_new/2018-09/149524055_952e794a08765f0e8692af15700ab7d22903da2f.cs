using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class UIAudioManager : MonoBehaviour
{
    public AudioClip click;
    public AudioClip score;
    public AudioClip gameOver;
    public AudioClip damage;

    AudioSource audioSource;

    // Use this for initialization
    void Start()
    {
        audioSource = GetComponent<AudioSource>();
    }

    // Update is called once per frame
    void Update()
    {

    }

    public void PlayClick()
    {

    }

    public void PlayScore()
    {

    }

    public void PlayGameOver()
    {

    }

    public void PlayDamage()
    {

    }
}