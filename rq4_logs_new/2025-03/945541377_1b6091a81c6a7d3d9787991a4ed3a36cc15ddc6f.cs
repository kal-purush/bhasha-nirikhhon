using UnityEngine;

public class AudioManager : MonoBehaviour
{
    [Header(".........Sources........")]
    [SerializeField] private AudioSource backgroundSource;
    [SerializeField] private AudioSource SFXSource;

    [Header(".........Background music clips.......")]
    [SerializeField] private AudioClip OrochimaruTheme;

    [Header("..........Player SFX clips............")]
    [SerializeField] public AudioClip ItachiFireballStart;
    [SerializeField] public AudioClip ItachiFireballEnd;
    [SerializeField] public AudioClip ItachiHurt;
    // Start is called once before the first execution of Update after the MonoBehaviour is created
    void Start()
    {
        backgroundSource.clip = OrochimaruTheme;
        backgroundSource.loop = true;
        backgroundSource.Play();
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    public void PlaySFX(AudioClip audioClip)
    {
        SFXSource.Stop(); // Stop any currently playing SFX
        SFXSource.clip = audioClip;
        SFXSource.Play();
    }
}