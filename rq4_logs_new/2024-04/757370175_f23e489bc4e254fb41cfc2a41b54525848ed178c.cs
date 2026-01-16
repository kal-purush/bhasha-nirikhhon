using UnityEngine;
using UnityEngine.Audio;
using System;
using System.Collections;
using System.Collections.Generic;

public enum SoundType
{
    SFX,
    Music,
    VoiceOver
}

[System.Serializable]
public class Sound
{
    public string name;
    public AudioClip clip;
    public SoundType type;
    [Range(0f, 1f)] public float volume = 1f;
    [Range(0.1f, 3f)] public float pitch = 1f;
    public bool loop;

    [HideInInspector] public AudioSource source;
}

public class AudioManager : MonoBehaviour
{
    public static AudioManager Instance;

    public List<Sound> sounds = new List<Sound>();

    public event Action OnVoiceOverFinished;

    public RectTransform rectTransform;

    void Awake()
    {
        if (Instance == null)
            Instance = this;
        else
        {
            Destroy(gameObject);
            return;
        }

        DontDestroyOnLoad(gameObject);

        foreach (Sound sound in sounds)
        {
            sound.source = gameObject.AddComponent<AudioSource>();
            sound.source.clip = sound.clip;
            sound.source.volume = sound.volume;
            sound.source.pitch = sound.pitch;
            sound.source.loop = sound.loop;
        }
    }

    public void Play(string name)
    {
        Sound sound = sounds.Find(s => s.name == name);
        if (sound == null)
        {
            Debug.LogWarning("Sound: " + name + " not found!");
            return;
        }

        sound.source.Play();
    }

    public void Stop(string name)
    {
        Sound sound = sounds.Find(s => s.name == name);
        if (sound == null)
        {
            Debug.LogWarning("Sound: " + name + " not found!");
            return;
        }

        sound.source.Stop();
    }

    public void Pause(string name)
    {
        Sound sound = sounds.Find(s => s.name == name);
        if (sound == null)
        {
            Debug.LogWarning("Sound: " + name + " not found!");
            return;
        }

        sound.source.Pause();
    }

    public void Resume(string name)
    {
        Sound sound = sounds.Find(s => s.name == name);
        if (sound == null)
        {
            Debug.LogWarning("Sound: " + name + " not found!");
            return;
        }

        sound.source.UnPause();
    }

    public bool IsPlaying(string name)
    {
        Sound sound = sounds.Find(s => s.name == name);
        if (sound == null)
        {
            Debug.LogWarning("Sound: " + name + " not found!");
            return false;
        }

        return sound.source.isPlaying;
    }

    public void PlayVoiceOver(string name)
    {
        Sound sound = sounds.Find(s => s.name == name && s.type == SoundType.VoiceOver);
        if (sound == null)
        {
            Debug.LogWarning("Voice Over: " + name + " not found!");
            return;
        }

        StartCoroutine(PlayVoiceOverCoroutine(sound));
    }

    IEnumerator PlayVoiceOverCoroutine(Sound voiceOver)
    {
        voiceOver.source.Play();

        while (voiceOver.source.isPlaying)
        {
            yield return null;
        }

        OnVoiceOverFinished?.Invoke();
    }
}