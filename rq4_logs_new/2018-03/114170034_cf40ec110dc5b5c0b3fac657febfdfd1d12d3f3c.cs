using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AudioManager : MonoBehaviour {

    public static AudioManager instance;

    public AudioSource musicPlayer;
    public AudioSource soundPlayer;

    public AudioClip mainMenuMusic;
    public AudioClip clickingSound;
    public AudioClip hoveredSound;

    private void Awake()
    {
        if (instance == null) {
            instance = this;
            DontDestroyOnLoad(this);
        }
        else Destroy(this);
    }

    public void playMainMenuMusic()
    {
        musicPlayer.clip = mainMenuMusic;
        musicPlayer.Play();
        musicPlayer.loop = true;
    }

    public void pauseMainMenuMusic()
    {
        musicPlayer.Pause();
    }

    public void playButtonClicked()
    {
        soundPlayer.clip = clickingSound;
        soundPlayer.Play();
    }

    public void playButtonHovered()
    {
        soundPlayer.clip = hoveredSound;
        soundPlayer.Play();
    }
}