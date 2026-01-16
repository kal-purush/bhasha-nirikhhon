using UnityEngine;
using System.Collections;

public class MusicManager : MonoBehaviour
{
    public static MusicManager instance;

    public AudioSource fightingMusic;
    public float fadeOutDuration = 3.0f; // Duration of the fade out in seconds

    private Coroutine fadeOutCoroutine;

    private void Awake()
    {
        if (instance == null)
        {
            instance = this;
        }
        else
        {
            Destroy(gameObject);
            return;
        }
    }

    public void PlayFightingMusic()
    {
        // Start playing the fighting music if it's not already playing
        if (!fightingMusic.isPlaying)
        {
            fightingMusic.Play();
        }
    }

    public void StopMusic()
    {
        // Fade out the fighting music when called
        if (fadeOutCoroutine != null)
        {
            StopCoroutine(fadeOutCoroutine);
        }
        fadeOutCoroutine = StartCoroutine(FadeOutMusic(fightingMusic));
    }

    private IEnumerator FadeOutMusic(AudioSource music)
    {
        float startVolume = music.volume;
        float currentTime = 0;

        while (currentTime < fadeOutDuration)
        {
            currentTime += Time.deltaTime;
            music.volume = Mathf.Lerp(startVolume, 0, currentTime / fadeOutDuration);
            yield return null;
        }

        music.Stop();
        music.volume = startVolume; // Reset volume for next use
    }
}