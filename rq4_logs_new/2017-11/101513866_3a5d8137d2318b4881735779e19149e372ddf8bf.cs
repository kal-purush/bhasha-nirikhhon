using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System;

public class Audio_Manager : MonoBehaviour
{
    #region Variables

    public AudioSource backgroundMusicSource;
    public AudioSource effectSoundSource;
    public AudioClip backroundMusic1;
    public AudioClip backroundMusic2;
    public AudioClip backroundMusic3;
    public AudioClip plantInGound1;
    public AudioClip plantInGound2;
    public AudioClip plantInGound3;
    public AudioClip plantInGound4;
    public AudioClip button;

    #endregion

    #region Singleton

    // create variable for storing singleton that any script can access
    private static Audio_Manager instance;

    // create AudioManager
    private Audio_Manager()
    {

    }

    // Property for Singleton
    public static Audio_Manager Instance
    {
        get
        {
            // If the singleton does not exist
            if (instance == null)
            {
                // create and return it
                instance = new Audio_Manager();
            }

            // otherwise, just return it
            return instance;
        }
    }

    #endregion

    #region Awake

    private void Awake()
    {
        if(GameObject.FindGameObjectsWithTag("AudioManager").Length > 1)
        {
            Destroy(gameObject);
        }

        DontDestroyOnLoad(gameObject);

        //add audio source
        backgroundMusicSource = gameObject.AddComponent<AudioSource>();
        effectSoundSource = gameObject.AddComponent<AudioSource>();

        //load sound files
        backroundMusic1 = Resources.Load<AudioClip>("Sounds/Background1");
        backroundMusic2 = Resources.Load<AudioClip>("Sounds/Background2");
        backroundMusic3 = Resources.Load<AudioClip>("Sounds/Background3");
        plantInGound1 = Resources.Load<AudioClip>("Sounds/plantInGround1");
        plantInGound2 = Resources.Load<AudioClip>("Sounds/plantInGround2");
        plantInGound3 = Resources.Load<AudioClip>("Sounds/plantInGround3");
        plantInGound4 = Resources.Load<AudioClip>("Sounds/plantInGround4");
        button = Resources.Load<AudioClip>("Sounds/ButtonClick");

        PlayBackgroundMusic();
    }

    #endregion

    #region Update

    private void Update()
    {
        if(!backgroundMusicSource.isPlaying)
        {
            PlayBackgroundMusic();
        }
    }

    #endregion

    #region Play Sounds

    public void PlayButtonSound()
    {
        effectSoundSource.PlayOneShot(button, .6f);
    }

    public void PlayBackgroundMusic()
    {
        backgroundMusicSource.Stop();
        int rand = UnityEngine.Random.Range(0, 3);

        switch (rand)
        {
            case 0:
                backgroundMusicSource.PlayOneShot(backroundMusic1, .2f);
                backgroundMusicSource.volume = .2f;
                break;
            case 1:
                backgroundMusicSource.PlayOneShot(backroundMusic2, .4f);
                backgroundMusicSource.volume = .4f;
                break;
            case 2:
                backgroundMusicSource.PlayOneShot(backroundMusic3, .4f);
                backgroundMusicSource.volume = .4f;
                break;
            default:
                backgroundMusicSource.PlayOneShot(backroundMusic1, .2f);
                backgroundMusicSource.volume = .2f;
                break;
        }
    }

    #endregion
}