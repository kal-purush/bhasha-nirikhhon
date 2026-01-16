using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using LeakyAbstraction; // Using sound manager
using UnityTimer;

public class PlayMusic : MonoBehaviour
{
    private bool isChangeMusic = false;
    private AudioSource sourceMusic1;


    // Start is called before the first frame update
    void Start()
    {
        SoundManager.Instance.PlaySound(GameSound.Music1);

        Timer.Register(42f, ChangeMusicToBass);
    }

    private void Update()
    {
        if (isChangeMusic && sourceMusic1.volume != 0)
        {
            sourceMusic1.volume = sourceMusic1.volume -= 0.3f * Time.deltaTime;
        }
    }

    // Update is called once per frame
    private void ChangeMusicToBass()
    {
        transform.GetChild(0).gameObject.SetActive(true);
        //transform.GetChild(1).gameObject.SetActive(false);

        sourceMusic1 = transform.GetChild(1).gameObject.GetComponent<AudioSource>();
        isChangeMusic = true;

        SoundManager.Instance.PlaySound(GameSound.Music2, playFinishedCallback : RepeatMusic);
    }

    private void RepeatMusic(GameSound gameSound)
    {
        SoundManager.Instance.PlaySound(gameSound, playFinishedCallback: RepeatMusic);
    }
}