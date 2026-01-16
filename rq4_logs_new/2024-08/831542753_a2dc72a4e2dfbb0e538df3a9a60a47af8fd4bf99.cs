using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class SettingsMenu : MonoBehaviour
{
    public Slider sensitivitySlider;
    public Slider volumeSlider;

    void Start()
    {
        sensitivitySlider.onValueChanged.AddListener(OnSensitivitySliderChanged);
        volumeSlider.onValueChanged.AddListener(OnVolumeSliderChanged);
    }

    private void OnSensitivitySliderChanged(float value)
    {
        PlayerLookController.SetMouseSens(sensitivitySlider.value * 40f);
    }

    private void OnVolumeSliderChanged(float value)
    {
        PlayerLookController.SetVolume(volumeSlider.value / 10f);
    }
}
