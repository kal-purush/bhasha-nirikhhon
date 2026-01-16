using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class crateCollisionSound : MonoBehaviour
{
    private AudioSource crateAudioSource;
    public AudioClip[] crateCollisionClips;
    // Start is called before the first frame update
    void Start()
    {
        crateAudioSource = GetComponent<AudioSource>();
    }

    private void OnCollisionEnter(Collision collision)
    {
        if (collision.relativeVelocity.magnitude > 2)
        {
            crateAudioSource.PlayOneShot
                (crateCollisionClips[UnityEngine.Random.Range(0, crateCollisionClips.Length - 1)], collision.relativeVelocity.magnitude);
        }
    }
}