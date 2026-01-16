using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class crateCollisionSound : MonoBehaviour
{
    private AudioSource crateAudioSource;
    public AudioClip[] crateCollisionClips;
    public float velocityThreshold, pitchMaximum;
    // Start is called before the first frame update
    void Start()
    {
        crateAudioSource = GetComponent<AudioSource>();
    }

    private void OnCollisionEnter(Collision collision)
    {
        int randomIndex = UnityEngine.Random.Range(0, crateCollisionClips.Length);
        if (collision.relativeVelocity.magnitude > velocityThreshold)
        {
            crateAudioSource.pitch = UnityEngine.Random.Range(scale(collision.relativeVelocity.magnitude, 0.0f, velocityThreshold, 0.7f, 0.5f), pitchMaximum);
            crateAudioSource.PlayOneShot
                (crateCollisionClips[randomIndex], collision.relativeVelocity.magnitude);
        }
        else
        {
            crateAudioSource.pitch = 1;
            crateAudioSource.PlayOneShot
                (crateCollisionClips[randomIndex], 0.7f);
        }

    }

    public float scale(float OldValue, float OldMin, float OldMax, float NewMin, float NewMax)
    {
        float OldRange = (OldMax - OldMin);
        float NewRange = (NewMax - NewMin);
        float NewValue = (((OldValue - OldMin) * NewRange) / OldRange) + NewMin;
        return (NewValue);
    }

}