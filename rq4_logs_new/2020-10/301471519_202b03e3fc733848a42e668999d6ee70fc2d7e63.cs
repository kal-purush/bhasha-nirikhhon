using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AudioContoller : MonoBehaviour
{
    public static AudioContoller instance;
    public AudioClip[] audioClips;
    internal AudioSource source;
    // Start is called before the first frame update
    void Start()
    {
        instance = this;
        source = gameObject.GetComponent<AudioSource>();

    }

    // Update is called once per frame
    void Update()
    {

    }

    public void WarningSound()
    {
        source.PlayOneShot(audioClips[0]);
        source.volume = 0.1f;
    }
   
}