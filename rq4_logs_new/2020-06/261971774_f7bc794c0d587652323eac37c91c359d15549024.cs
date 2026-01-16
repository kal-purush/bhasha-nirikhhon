using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BattleAudio : MonoBehaviour
{
    public AudioClip tutorial;
    public AudioClip levelone;
    public AudioClip leveltwo;
    public AudioClip levelthree;
    void Start()
    {
        switch (LevelManager.Level){
            case "tutorial":
                gameObject.GetComponent<AudioSource>().clip = tutorial;
                break;
            case "one":
				gameObject.GetComponent<AudioSource>().clip = levelone;
                break;
            case "two":
                gameObject.GetComponent<AudioSource>().clip = leveltwo;
                break;
            case "three":
                gameObject.GetComponent<AudioSource>().clip = levelthree;
                break;
        }
        gameObject.GetComponent<AudioSource>().Play();
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}