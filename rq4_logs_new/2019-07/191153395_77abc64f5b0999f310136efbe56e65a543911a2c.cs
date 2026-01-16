using System.Collections;
using System.Collections.Generic;
using UnityEngine;

/*
 * 
 * This class needs to be attached to the game object along with AudioSource component.
 * 
 */
public class SoundEffect : MonoBehaviour {

    //Audio sources
    AudioSource audio;
    public AudioClip[] BGM;
    public AudioClip[] Lane;
    public AudioClip[] Jump;
    public AudioClip[] Hit;
    public AudioClip[] Coin;

    private void Start()
    {
        audio = GetComponent<AudioSource>();
    }

    void PlaySound(AudioClip[] sounds)
    {
        int randomed = Random.Range(0, sounds.Length - 1);
        audio.PlayOneShot(sounds[randomed]);
    }

    public void PlayBGM()
    {
        PlaySound(BGM);
    }

    public void PlayLane()
    {
        PlaySound(Lane);
    }

    public void PlayJump()
    {
        PlaySound(Jump);
    }

    public void PlayHit()
    {
        PlaySound(Hit);
    }

    public void PlayCoin()
    {
        PlaySound(Coin);
    }

}