using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AudioSelector : MonoBehaviour
{
    public AudioClip[] soundtracks;
    public AudioSource audio;

    // Start is called before the first frame update
    void Start()
    {
        randomAudio();
    }

    // Update is called once per frame
    void Update()
    {
        
    }


    public void randomAudio() {
        int audioToSelect =  Random.Range(0, soundtracks.Length);
        audio.clip = soundtracks[audioToSelect];
        audio.Play();
    }

}