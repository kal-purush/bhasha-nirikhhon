using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class SoundManager : MonoBehaviour {

    public static SoundManager Instance = null;
    public AudioSource sfxSource;
    public AudioSource bgmSource;
    public AudioClip bgm;       

    private void Awake() {
        // Singleton setup for GameManager
        if (Instance == null)
            Instance = this;
        else if (Instance != this)
            Destroy(gameObject);

        DontDestroyOnLoad(gameObject);
    }

    /// <summary>
    /// If the background music ends, replay it
    /// </summary>
    private void Update() {
        if (!bgmSource.isPlaying)
            PlayBgm();
    }


    /// <summary>
    /// Play Once audio
    /// </summary>
    /// <param name="clip"></param>
    public void PlaySfx(AudioClip clip) {
        sfxSource.PlayOneShot(clip);
    }

    /// <summary>
    /// Play Background track
    /// </summary>
    /// <param name="track"></param>
    public void PlayBgm() {
        bgmSource.Play();
    }

}