using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BackgroundMusicController : MonoBehaviour
{
    [SerializeField] private AudioClip chillStart;
    [SerializeField] private AudioClip chillLoop;
    [SerializeField] private AudioClip tenseStart;
    [SerializeField] private AudioClip tenseLoop;
    [SerializeField] private AudioClip riotLoop;
    [SerializeField] private List<AudioSource> audioSourceArray;

    private int selectedAudioSourceIndex = 0;
    private Office _office;
    private OfficeState previousOfficeState;
    private BackgroundMusicState state;
    private double nextStartTime;
    private AudioClip nextClip;

    private void Start()
    {
        _office = FindObjectOfType<Office>();
        state = BackgroundMusicState.Intro;
        nextStartTime = AudioSettings.dspTime + 0.5;
        previousOfficeState = OfficeState.Chill;
        nextClip = GetNextClip();
        playNext(nextClip);
    }

    void Update()
    {
        if (AudioSettings.dspTime > nextStartTime - 1)
        {
            nextClip = GetNextClip();
            Debug.Log("Office State: " + _office.State);
            Debug.Log("Next Clip: " + nextClip.name);
            playNext(nextClip);
        }
    }

    private void playNext(AudioClip clipToPlay)
    {
        // Loads the next Clip to play and schedules when it will start
        audioSourceArray[selectedAudioSourceIndex].clip = clipToPlay;
        audioSourceArray[selectedAudioSourceIndex].PlayScheduled(nextStartTime);

        // Checks how long the Clip will last and updates the Next Start Time with a new value
        double duration = (double) clipToPlay.samples / clipToPlay.frequency;
        nextStartTime = nextStartTime + duration;

        // Switches the selectedAudioSourceIndex to use the other Audio Source next
        selectedAudioSourceIndex = 1 - selectedAudioSourceIndex;
    }

    private AudioClip GetNextClip()
    {
        if (previousOfficeState != _office.State)
        {
            state = BackgroundMusicState.Intro;
        }

        switch (_office.State)
        {
            case OfficeState.Chill:
                if (state == BackgroundMusicState.Intro)
                    nextClip = chillStart;
                else
                    nextClip = chillLoop;
                break;
            case OfficeState.Tense:
                if (state == BackgroundMusicState.Intro)
                    nextClip = tenseStart;
                else
                    nextClip = tenseLoop;
                break;
            case OfficeState.Riot:
                nextClip = riotLoop;
                break;
        }

        if (state == BackgroundMusicState.Intro)
        {
            state = BackgroundMusicState.Loop;
        }

        return nextClip;
    }
}