using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class SoundController : MonoBehaviour
{
    public GameObject waveManager;
    public AudioClip levelOne;
    public AudioClip boss;
    public AudioClip transition;
    bool isTransition;
    [SerializeField] AudioSource source;
    private void Start()
    {
        source.clip = levelOne;
        source.Play();
    }
    void Update()
    {
        if(waveManager.GetComponent<WaveManager>().waveType == 10 && !isTransition)
        {
            isTransition = true;
            source.clip = transition;
            source.Play();
            source.loop = false;
            StartCoroutine(WaitForAudio(transition));
        }
    }

    private IEnumerator WaitForAudio(AudioClip clip)
    {
        while (source.isPlaying)
        {
            yield return null;
        }
        PlayBossMusic();
    }

    void PlayBossMusic()
    {
        source.loop = true;
        source.clip = boss;
        source.Play();
    }
}