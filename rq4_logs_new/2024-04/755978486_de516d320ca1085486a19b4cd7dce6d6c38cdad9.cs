using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.SceneManagement;
public class VolumeSetting : MonoBehaviour
{
    [SerializeField] private Slider musicSlider;
    [SerializeField] private Slider sfxSlider;
    private float music_volume;
    private float sfx_volume;

    private void Awake()
    {
        sfxSlider.value = 0.5f;
        musicSlider.value = 0.5f;

    }
    public void SetMusicVolume()
    {
        music_volume = musicSlider.value;
        PlayerPrefs.SetFloat("music_volume_multiplier", music_volume);
        PlayerPrefs.Save();
        
    }
    public void SetSFXVolume()
    {
        sfx_volume = sfxSlider.value;
        PlayerPrefs.SetFloat("sfx_volume_multiplier", sfx_volume);
        PlayerPrefs.Save();

    }
    public void BackToMainMenu()
    {
        SceneManager.LoadSceneAsync(0);

    }
}