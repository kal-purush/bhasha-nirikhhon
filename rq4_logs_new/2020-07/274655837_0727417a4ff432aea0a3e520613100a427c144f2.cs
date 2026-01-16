using System;
using System.Collections;
using System.Collections.Generic;
using System.Media;
using UnityEngine;

public static class AudioManager 
{
    public enum Audio
    {
        click,
        checker_hit,
        zoom,
        unzoom
    }

    public static void PlaySound(Audio audio)
    {
        GameObject audioGameObject = new GameObject("Audio");
        AudioSource audioSource = audioGameObject.AddComponent<AudioSource>();
        AudioClip audioClip = GetAudioClip(audio);
        audioSource.PlayOneShot(audioClip);
        UnityEngine.Object.Destroy(audioGameObject, audioClip.length);
    }


    private static AudioClip GetAudioClip(Audio audio)
    {
        foreach(AudioAssets.SoundAudioClip soundAudioClip in AudioAssets.i.SoundAudioClipArray)
        {
            if (soundAudioClip.audio == audio)
                return soundAudioClip.audioClip;
        }
        Debug.LogError("Sound " + audio + " not found!");
        return null;
    }
}