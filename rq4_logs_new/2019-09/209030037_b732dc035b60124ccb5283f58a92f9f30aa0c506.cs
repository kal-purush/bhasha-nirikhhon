using System;
using System.Collections;
using System.Collections.Generic;
using Assets.GameSystem.Constant;
using UnityEngine;
using Assets.GameSystem.Constant.Enum;
using Assets.GameSystem.Entity;
using Assets.GameSystem.Service;
using TMPro;
using UnityEngine.UI;

public class SettingsManager : MonoBehaviour
{
    public GameObject volumeBar;

    public GameObject lightingBar;

    public GameObject fieldOfViewBar;

    public GameObject volumeAmount;

    public GameObject lightingAmount;

    public GameObject fieldOfViewAmount;

    private GameSettings gameSettings;

    private void Awake()
    {
        gameSettings = PreferencesService.LoadSettings();
    }

    // Start is called before the first frame update
    void Start()
    {
        SetSettings(gameSettings.Volume, gameSettings.Lighting, gameSettings.FieldOfView);
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    private void SetSettings(int volume, int lighting, int fov)
    {
        volumeBar.GetComponent<Image>().fillAmount = PreferencesService.ConvertToUnityValue(volume);
        lightingBar.GetComponent<Image>().fillAmount = PreferencesService.ConvertToUnityValue(lighting);
        fieldOfViewBar.GetComponent<Image>().fillAmount = PreferencesService.ConvertToFOV(fov);
        volumeAmount.GetComponent<TextMeshProUGUI>().text = volume.ToString();
        lightingAmount.GetComponent<TextMeshProUGUI>().text = lighting.ToString();
        fieldOfViewAmount.GetComponent<TextMeshProUGUI>().text = fov.ToString();
    }
    

    public void Min(int action)
    {
        if ((int) PreferencesEnum.Volume == action)
        {
            if (GameConstants.MIN_VOLUME != gameSettings.Volume)
            {
                gameSettings.minVolume();
            }
        }
        else if((int) PreferencesEnum.Lighting == action)
        {
            if (GameConstants.MIN_LIGHTING != gameSettings.Lighting)
            {
                gameSettings.minLighting();
            }
        }
        else if ((int) PreferencesEnum.FieldOfView == action)
        {
            if (GameConstants.MIN_FOV != gameSettings.FieldOfView)
            {
                gameSettings.minFOV();
            }
        }
        else
        {
           throw new Exception("Invalid Parameters");
        }
        SetSettings(gameSettings.Volume, gameSettings.Lighting, gameSettings.FieldOfView);
    }

    public void Plus(int action)
    {
        if ((int) PreferencesEnum.Volume == action)
        {
            if (GameConstants.MAX_VOLUME != gameSettings.Volume)
            {
                gameSettings.plusVolume();
            }
        }
        else if((int) PreferencesEnum.Lighting == action)
        {
            if (GameConstants.MAX_LIGHTING != gameSettings.Lighting)
            {
                gameSettings.plusLighting();
            }
        }
        else if ((int) PreferencesEnum.FieldOfView == action)
        {
            if (GameConstants.MAX_FOV != gameSettings.FieldOfView)
            {
                gameSettings.plusFOV();
            }
        }
        else
        {
            throw new Exception("Invalid Parameters");
        }
        SetSettings(gameSettings.Volume, gameSettings.Lighting, gameSettings.FieldOfView);
    }

    public void Apply()
    {
        PreferencesService.WriteSettings(gameSettings);
    }
}