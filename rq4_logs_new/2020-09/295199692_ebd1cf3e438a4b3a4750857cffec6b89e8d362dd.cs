using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[RequireComponent(typeof(AudioSource))]
public class SoundManager : MonoBehaviour
{
    public static SoundManager i;

    public SoundConfig musicConfig;

    public List<AudioConfig> clips;
    public List<AudioClip> backgroundMusic;

    private AudioSource musicSource;
    private AudioSource source;

    private int musicIndex;

    private IDictionary<string, AudioConfig> sources;

    // Start is called before the first frame update
    void Start()
    {
        if (i == null)
        {
            i = this;
        } else
        {
            Destroy(gameObject);
            return; 
        }

        var allSources = GetComponents<AudioSource>();
        source = allSources[0];
        musicSource = allSources[1];

        musicSource.volume = musicConfig.volume;

        sources = new Dictionary<string, AudioConfig>();
        foreach (var clip in clips)
        {
            if (sources.ContainsKey(clip.name))
            {
                continue;
            }

            sources.Add(clip.name, clip);
        }

        musicIndex = 0;
        PlayNextSong();

        DontDestroyOnLoad(gameObject);
    }

    void Update()
    {
        musicSource.volume = musicConfig.volume;
    }

    public void PlayNextSong()
    {
        musicSource.Stop();

        var clip = backgroundMusic[musicIndex];
        musicSource.clip = clip;
        musicIndex = (musicIndex + 1) % backgroundMusic.Count;

        musicSource.Play();
        Invoke("PlayNextSong", musicSource.clip.length);
    }

    public void PauseAll()
    {
        musicSource.Pause();
        source.Pause();
    }

    public void UnpauseAll()
    {
        musicSource.UnPause();
        source.UnPause();
    }

    public void PlaySound(string soundName)
    {
        var clip = sources["default"];
        if (!sources.ContainsKey(soundName))
        {
            Debug.LogError($"Could not find AudioClip with name '{soundName}'");
        } else
        {
            clip = sources[soundName];
        }

        source.clip = clip.clip;
        source.volume = clip.volume;
        source.pitch = clip.pitch;

        source.Play();
    }
}