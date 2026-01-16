using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Footsteps : MonoBehaviour
{
    public Player player;
    private AudioSource playerAudioSource;
    public AudioClip[] metalFootstepClips;
    public AudioClip[] moonFootstepClips;
    void Start()
    {
        playerAudioSource = player.GetComponent<AudioSource>();
    }
    private void Footstep()
    {
        AudioClip[] stepSounds;
        RaycastHit hit;
        Physics.Raycast(player.transform.position, Vector3.down, out hit);
        switch(hit.transform.gameObject.tag) {
            default:
            case "MetalFloor":
                stepSounds = metalFootstepClips;
            break;
            case "MoonFloor":
                stepSounds = moonFootstepClips;
            break;
        }
        playerAudioSource.PlayOneShot(stepSounds[UnityEngine.Random.Range(0, stepSounds.Length - 1)], 1.0f);
    }
}