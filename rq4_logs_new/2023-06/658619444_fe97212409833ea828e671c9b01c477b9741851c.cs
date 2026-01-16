using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AudioPlayer : MonoBehaviour
{
    [Header("Shooting")]
    [SerializeField] AudioClip shootingSFX;
    [SerializeField] [Range(0f, 1f)] float shootingVolume = 1f;

    [Header("Damage")]
    [SerializeField] AudioClip damageSFX;
    [SerializeField][Range(0f, 1f)] float damagingVolume = 1f;

    void PlayClip(AudioClip clip, float volume)
    {
        if (clip != null)
        {
            Vector3 cameraPos = Camera.main.transform.position;
            AudioSource.PlayClipAtPoint(clip, cameraPos, volume);
        }
    }

    public void PlayShootingClip()
    {
        PlayClip(shootingSFX, shootingVolume);
    }

    public void PlayDamagingClip()
    {
        PlayClip(damageSFX, damagingVolume);
    }
}