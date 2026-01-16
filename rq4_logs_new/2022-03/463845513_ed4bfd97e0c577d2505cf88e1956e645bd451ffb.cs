using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class AudioSettingsMenu : MonoBehaviour
{
    public Slider slider;
    public float sliderValue;
    public Image imageMute;
    // Start is called before the first frame update
    void Start()
    {
        slider.value = PlayerPrefs.GetFloat("volumeAudio", 0.5f);
        AudioListener.volume = slider.value;
        checkIfMuteIsOn();
        
    }

    public void ChangeSlider(float value)
    {
        sliderValue = value;
        PlayerPrefs.SetFloat("volumeAudio", sliderValue);
        AudioListener.volume = slider.value;
        checkIfMuteIsOn();
    }

    public void checkIfMuteIsOn()
    {
        if (sliderValue == 0) imageMute.enabled = true;
        else imageMute.enabled = false;
    }
}