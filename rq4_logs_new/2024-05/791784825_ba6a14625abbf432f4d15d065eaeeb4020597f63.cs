using UnityEngine;

public class AnimationSoundManager : MonoBehaviour
{
    public AudioSource audioSource;
    public AudioClip[] audioClips;

    void Start()
    {
        if (audioSource == null)
        {
            audioSource = GetComponent<AudioSource>();
        }
    }

    // This method will be called from the Animation Event
    public void PlaySound(string soundName)
    {
        AudioClip clip = GetAudioClipByName(soundName);
        if (clip != null)
        {
            audioSource.PlayOneShot(clip);
        }
        else
        {
            Debug.LogWarning("Sound not found: " + soundName);
        }
    }

    private AudioClip GetAudioClipByName(string soundName)
    {
        foreach (AudioClip clip in audioClips)
        {
            if (clip.name == soundName)
            {
                return clip;
            }
        }
        return null;
    }
}