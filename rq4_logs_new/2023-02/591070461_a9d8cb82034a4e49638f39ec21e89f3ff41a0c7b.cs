using System;
using UnityEngine;

public class AudioManager : MonoBehaviour
{
    private static AudioManager instance = null;
    public static AudioManager Instance { get { return instance; } }
    public AudioSource soundFx; // Button click, footsteps
    public AudioSource music; // BG music, GameOver
    public Sound[] sounds;
    void Awake()
    {
        if (instance != null)
        {
            Destroy(instance);
            return;
        }
        else
        {
            instance = this;
            DontDestroyOnLoad(instance);
        }
    }
    public void PlaySound(SoundType soundType)
    {
        Sound sound = Array.Find(sounds, item => item.soundType == soundType);
        if (soundType == SoundType.BGMusic || soundType == SoundType.GameOver)
        {
            music.clip = sound.audioClip;
            music.Play();
        }
        else
        {
            soundFx.PlayOneShot(sound.audioClip);
        }
    }
    public float GetSfxVolume()
    {
        return soundFx.volume;
    }
    public void SetSfxVolume(float _volume)
    {
        soundFx.volume = _volume;
    }
    public float GetMusicVolume()
    {
        return music.volume;
    }
    public void SetMusicVolume(float _volume)
    {
        music.volume = _volume;
    }
}
[Serializable]
public class Sound
{
    public SoundType soundType;
    public AudioClip audioClip;
}
public enum SoundType
{
    ButtonClick, BGMusic, GameOver, Footstep, Jump
}