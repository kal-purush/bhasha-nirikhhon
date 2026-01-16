using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class LobbyMusicManager : MonoBehaviour
{
    [SerializeField] AudioClip[] audioClips;
    bool audio = false;

    private void Update()
    {
        if (!audio)
        {
            int index = Random.Range(0, audioClips.Length - 1);

            if (TryGetComponent<AudioSource>(out AudioSource audioSource))
            {
                audioSource.clip = audioClips[index];
                audioSource.Play();
            }

            audio = true;
        }        
    }
}