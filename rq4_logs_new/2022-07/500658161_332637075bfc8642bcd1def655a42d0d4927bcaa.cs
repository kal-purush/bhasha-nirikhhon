using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AudioManager : Singleton<AudioManager>
{
    [SerializeField]
    private AudioClip pickTank, countDownFight, hitSound;

    private static AudioSource audioSrc;

    // Start is called before the first frame update
    void Start()
    {
        audioSrc = GetComponent<AudioSource>();
    }

    public void PlaySoundOneShot(string soundType)
    {
        switch (soundType)
        {
            case "pickTank":
                audioSrc.PlayOneShot(pickTank);
                break;
            case "countDownFight":
                audioSrc.PlayOneShot(countDownFight);
                break;
            case "hitSound":
                audioSrc.PlayOneShot(hitSound);
                break;
        }
    }

    public void PlayBackgroundSound(string soundType)
    {
        switch (soundType)
        {
            case "pickTank":
                audioSrc.PlayOneShot(pickTank);
                break;
            case "countDownFight":
                audioSrc.PlayOneShot(countDownFight);
                break;
            case "hitSound":
                audioSrc.PlayOneShot(hitSound);
                break;
        }
    }

    // Update is called once per frame
    void Update()
    {

    }
}