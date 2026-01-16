using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class SettingsMenu : MonoBehaviour
{
    public Slider sensitivitySlider;
    float sensitivity;

    void Start()
    {
        sensitivity = PlayerPrefs.GetFloat("sensitivity", 200f);
        sensitivitySlider.value = sensitivity;
    }

    void Update()
    {
        if (sensitivitySlider.value != sensitivity)
        {
            sensitivity = sensitivitySlider.value;
            PlayerPrefs.SetFloat("sensitivity", sensitivity);
        }
    }
}