using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerAudio : MonoBehaviour
{

    [SerializeField] private AudioSource[] audioSource;
    [SerializeField] private AudioClip audioAttack;
    [SerializeField] private AudioClip audioWalk;
    [SerializeField] private AudioClip audioJump;
    [SerializeField] private AudioClip audioLand;
    [SerializeField] private AudioClip audioHurt;
    
    // Start is called before the first frame update
    void Start()
    {
        audioSource = GetComponents<AudioSource>();
    }

    public void PlayAudioAttack()
    {
        audioSource[0].PlayOneShot(audioAttack);
    }
    
    public void PlayAudioJump()
    {
        audioSource[0].PlayOneShot(audioJump);
    }
    
    public void PlayAudioLand()
    {
        audioSource[0].PlayOneShot(audioLand);
    }
    
    public void PlayAudioHurt()
    {
        audioSource[0].PlayOneShot(audioHurt);
    }

    public void PlayWalking()
    {
        audioSource[1].clip = audioWalk;
        if(!audioSource[1].isPlaying) audioSource[1].Play();
    }

    public void StopWalking()
    {
        if(audioSource[1].isPlaying) audioSource[1].Stop();
    }
}