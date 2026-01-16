using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class sfxManager : MonoBehaviour
{
    public AudioClip[] clip;
    public int[] SentancesStart;
    public int currentSentance;
    public int currentClip;
    AudioSource source;

    void Awake(){
        source = GetComponent<AudioSource>();
    }

    void Start()
    {
        Randomise();
    }

    public void PlaySound(){
        source.clip = GetClip();
        source.Play();
    }

    public AudioClip GetClip()
    {
        if (currentClip + 1 != clip.Length)
        {
            currentClip++;
            if (currentSentance + 1 != SentancesStart.Length)
            {
                if (currentClip == SentancesStart[currentSentance + 1])
                    Randomise();
            }
            else
                Randomise();
        }
        else
            Randomise();
        return clip[currentClip];
    }

    void Randomise()
    {
        currentSentance = Random.Range(0, SentancesStart.Length);
        currentClip = SentancesStart[currentSentance];

    }
}