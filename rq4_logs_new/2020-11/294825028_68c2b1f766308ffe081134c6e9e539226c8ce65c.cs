using UnityEngine.UI;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Audio;



public class AudioManager : MonoBehaviour
{
    //Music source
    public AudioSource musicSource;
    public AudioMixer mixer;

    

    //Singleton
    public static AudioManager instance = null;

    //Default volume values - work w/ later 
    public float defaultMasterVol = 1.0f;
    public float defaultSFXVol = 1.0f;
    public float defaultMusicVol = 1.0f;

    //Mixer volume volumes - work w/ later
    float masterVol = 1.0f;
    float sfxVol = 1.0f;
    float musicVol = 1.0f;

    //check if AM instance exists
    private void Awake()
    {
        if(instance == null)
        {
            instance = this;
        }
        else if(instance != null)
        {
            Destroy(gameObject);
        }

        
        //Deals with not destroying GO on reload - used so music doesn't stop after changing scenes
        DontDestroyOnLoad(gameObject);
    }

    //Play Music
    public void PlayMusic(AudioClip clip)
    {
        musicSource.clip = clip;
        musicSource.Play();
    }
    //Implement later
    public void ResetEverything()
    {
        masterVol = defaultMasterVol;
        sfxVol = defaultSFXVol;
        musicVol = defaultMusicVol;

        var sliders = FindObjectsOfType<Slider>();
        foreach (Slider slider in sliders)
        {
            if(slider.tag == "Volume")
            {
                slider.value = 1;
            }
        }
    }
    public void ChangeVolume(string sliderName, float newVol)
    {
        if (sliderName == "MusicVol")
        {
            musicVol = Mathf.Clamp(newVol, 0, 1);
        }
        else if (sliderName == "SFXVol")
        {
            sfxVol = Mathf.Clamp(newVol, 0, 1);
        }
    }
}