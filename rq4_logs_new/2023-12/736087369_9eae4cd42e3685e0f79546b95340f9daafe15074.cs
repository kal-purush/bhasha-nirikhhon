using LethalSettings.UI.Components;
using LethalSettings.UI;
using System;
using System.Collections.Generic;
using System.Text;
using UnityEngine;
using Unity.Netcode;
using BepInEx.Configuration;

namespace PortableMultiTool;

internal class SettingsMenu : MonoBehaviour
{
    private static SettingsMenu Instance { get; set; }

    private SliderComponent costSlider, hackTimeSlider, batteryLifeSlider;

    private bool IsInGame
    {
        get => NetworkManager.Singleton != null && NetworkManager.Singleton.IsClient;
    }

    private void Awake()
    {
        if (Instance == null)
        {
            Instance = this;
        }
        else if (Instance != this)
        {
            Destroy(gameObject);
            return;
        }

        DontDestroyOnLoad(gameObject);
        gameObject.hideFlags = HideFlags.HideAndDontSave;
    }

    private void Start()
    {
        costSlider = new SliderComponent
        {
            Text = "Hack Pad Cost: $",
            DefaultValue = Config.hackPadCost.Value,
            MinValue = 10,
            MaxValue = 150,
            WholeNumbers = true,
            ShowValue = true,
            OnValueChange = (self, value) => Config.hackPadCost.Value = (int)value
        };
        hackTimeSlider = new SliderComponent
        {
            Text = "Hack Duration (seconds):",
            DefaultValue = Config.hackPadHackDuration.Value,
            MinValue = 1,
            MaxValue = 20,
            WholeNumbers = true,
            ShowValue = true,
            OnValueChange = (self, value) => Config.hackPadHackDuration.Value = (int)value
        };
        batteryLifeSlider = new SliderComponent
        {
            Text = "Battery Life (seconds):",
            DefaultValue = Config.hackPadBatteryLife.Value,
            MinValue = 10,
            MaxValue = 120,
            WholeNumbers = true,
            ShowValue = true,
            OnValueChange = (self, value) => Config.hackPadBatteryLife.Value = (int)value
        };
        ModMenu.RegisterMod(new ModMenu.ModSettingsConfig
        {
            Name = "Hack Pad",
            Id = PortableMultiToolBase.MODGUID,
            Description = "Adds the \"Hack Pad\" item as purchasable equipment allowing the holder to disable turrets and open/close Big Doors they come across while scrap hunting.",
            MenuComponents = [
                costSlider,
                hackTimeSlider,
                batteryLifeSlider,
                new ButtonComponent
                {
                    Text = "Restore Defaults",
                    OnClick = ResetDefaults
                }
            ]
        });
    }

    private void Update()
    {
        costSlider.Enabled = hackTimeSlider.Enabled = batteryLifeSlider.Enabled = !IsInGame;
    }

    private void ResetDefaults(ButtonComponent component)
    {
        Config.hackPadCost.Value = (int)Config.hackPadCost.DefaultValue;
        costSlider.CurrentValue = Config.hackPadCost.Value;

        Config.hackPadHackDuration.Value = (float)Config.hackPadHackDuration.DefaultValue;
        hackTimeSlider.CurrentValue = Config.hackPadHackDuration.Value;

        Config.hackPadBatteryLife.Value = (float)Config.hackPadBatteryLife.DefaultValue;
        batteryLifeSlider.CurrentValue = Config.hackPadBatteryLife.Value;
    }
}