using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Audio;
/// <summary>
/// The SoundModule class works as a mini sound manager for objects that need 3d audio.
/// It works almost the same as the audiomanager, the only difference is that it only works with one
/// AudioSource. To play a sound it receives an int, this ideally must be casted from an enum,
/// that enum should be in each class that uses the soundModule component, the enum will hold all the possible
/// sounds. This was done for organizational purposes and having a clear view in code of which sound is being 
/// played.
/// </summary>
[RequireComponent(typeof(AudioSource))]
public class SoundModule : MonoBehaviour
{
    public Sound[] sounds;
    AudioSource source;
    int index;
    private void Awake()
    {
        source = GetComponent<AudioSource>();
    }
    /// <summary>
    /// Play a sound, the integer must be casted from an enum inside the class.
    /// </summary>
    /// <param name="i"></param>
    public void Play(int i)
    {
        if(i < sounds.Length)
        {
            index = i;
            source.Stop();
            setAudioSource(sounds[i]);
            source.Play();
        }
        else{ Debug.LogWarning("SOUND NOT FOUNDED AT INDEX: " + i);}
    }
    /// <summary>
    /// Stops the current Sound
    /// </summary>
    public void StopCurrent()
    {
        source.Stop();
    }
    
    void setAudioSource(Sound sound)
    {
        this.source.clip = sound.clip;
        this.source.volume = sound.volume;
        this.source.pitch = sound.pitch;
        this.source.loop = sound.loop;
        this.source.outputAudioMixerGroup = AudioManager.Instance.mixers[(int)sound.mixerChannel];
    }

    private void OnValidate()
    {
        source = GetComponent<AudioSource>();
        source.spatialBlend = 1;
        source.playOnAwake = false;
    }
}