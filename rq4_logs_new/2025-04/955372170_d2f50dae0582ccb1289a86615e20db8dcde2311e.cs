using UnityEngine;

public class Music : MonoBehaviour
{
    public AudioSource audioSource { get; private set; }
    public AudioClip music;
    public AudioClip hurryMusic;
    public AudioClip introMusic;

    private AudioClip defaultMusic;
    private AudioClip overrideMusic = null;

    private bool isStopped = false;

    private void Awake()
    {
        audioSource = GetComponent<AudioSource>();
        defaultMusic = music;
    }
    private void Start()
    {
        audioSource.clip = music;
        audioSource.Play();
    }

    public void PlayMusic(AudioClip music, float duration = -1)
    {
        audioSource.clip = overrideMusic ?? music;
        audioSource.loop = true;
        audioSource.Play();
        isStopped = false;

        if (duration > 0)
        {
            Invoke(nameof(SwitchMusicBack), duration);
        } else
        {
            defaultMusic = music;
        }
    }

    public void PlayOverrideMusic(AudioClip music, float duration)
    {
        overrideMusic = music;
        PlayMusic(music, duration);
    }

    private void SwitchMusicBack()
    {
        if (!isStopped)
        {
            overrideMusic = null;
            PlayMusic(defaultMusic, -1);
        }
    }

    public void StopMusic()
    {
        audioSource.Stop();
        audioSource.loop = false;
        isStopped = true;
    }
}