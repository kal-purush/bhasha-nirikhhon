using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[RequireComponent(typeof(AudioSource))]
public class AudioController : MonoBehaviour
{
    public AudioClip sound; //gestion du son � jouer
    // barre de gestion du volume
    [Range(0f, 1f)]
    public float volume;

    public AudioSource source;
    public float tempo; // tempo de la musique en BPM (� renseigner)
    public float secPerBeat;
    public float musicStart;
    public float musicTime;
    public float musicBeats;
    private float sensibilite = 0.3f;
    public bool canMove = false;
    public bool movesBlocked = false;

    private void Start()
    {
        //gameObject.AddComponent<AudioSource>();
        source = GetComponent<AudioSource>();
        source.clip = sound;
        source.volume = volume;

        secPerBeat = 60f / tempo; //calcul du nombre de secondes dans chaque beat
        musicStart = (float)AudioSettings.dspTime; // temps o� la musique commence, conversion dspTime de double en float
        source.Play(); // d�marre la musique
    }
    private void Update()
    {
        musicTime = (float)(AudioSettings.dspTime - musicStart);
        musicBeats = musicTime / secPerBeat;

        source.volume = volume;

        if(movesBlocked==true)
        {
            canMove = false;
        }
        else if (musicBeats % 1 <= sensibilite)
        {
            canMove = true;
        }
        else
        {
            canMove = false;
        }
    }
}