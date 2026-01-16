using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class SoundManager : MonoBehaviour
{

    public AudioSource BackgroundMusic;
    public AudioSource PlayerFire;
    public AudioSource OpponentFire;
    public AudioSource PlayerExplosion;
    public AudioSource OpponentExplosion;

    private static SoundManager _instance;

    public static SoundManager Instance { get { return _instance; } }

    private void Awake()
    {
        if (_instance != null && _instance != this)
        {
            Destroy(this.gameObject);
        }
        else
        {
            _instance = this;
        }
    }
}