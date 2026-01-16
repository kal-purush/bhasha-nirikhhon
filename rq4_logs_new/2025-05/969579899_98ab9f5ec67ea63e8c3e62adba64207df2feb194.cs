using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class PopupSettingMusic : BasePopup
{
    [Header("Attribute")]
    [SerializeField] private AudioClip currentMusicSound;
    [SerializeField] private GlobalConfig globalConfig;
    [SerializeField] private List<SetupMusic> setupMusics;

    [Header("Buttons && Toggles")]
    [SerializeField] private Button exitButton;
    [SerializeField] private Toggle randomMusicTgl;

    private void Start()
    {
        SetListMusic();
        exitButton.onClick.AddListener(OnClickExitButton);
    }
    private void OnDestroy()
    {
        for (int i = 0; i < setupMusics.Count; i++)
        {
            setupMusics[i].OnSelected -= HandleMusicSelected;
        }
    }

    private void OnClickExitButton()
    {
        if (randomMusicTgl.isOn)
        {
            CheckStateToggleMusic(randomMusicTgl, false);
            int previousIndex = globalConfig.soundMusics.IndexOf(currentMusicSound);
            int newIndex;
            do
            {
                newIndex = Random.Range(0, globalConfig.soundMusics.Count);
            }
            while (newIndex == previousIndex);
            currentMusicSound = globalConfig.soundMusics[newIndex];
            if (AudioManager.HasInstance)
            {
                AudioManager.Instance.PlayBGM(currentMusicSound.name);
            }
        }
        this.Hide();
    }
    private void SetListMusic()
    {
        for (int i = 0; i < setupMusics.Count; i++)
        {
            setupMusics[i].SetCurrentMusic(globalConfig.soundMusics[i]);
            setupMusics[i].OnSelected += HandleMusicSelected;
        }
    }
    private void HandleMusicSelected(SetupMusic selectedItem)
    {
        CheckStateToggleMusic(selectedItem.GetToggleMusic(), true);
    }    

    public void CheckStateToggleMusic(Toggle current, bool isRandomMusic)
    {

        for (int i = 0; i < setupMusics.Count; i++)
        {
            if (!isRandomMusic)
            {
                if (setupMusics[i].GetStateToggleMusic() == true)
                {
                    setupMusics[i].OnClickValueChangePlayMusic(false);
                    setupMusics[i].SetStateToggleMusic(false);
                    setupMusics[i].HideReplayMusic();
                }
            }
            else
            {
                if (setupMusics[i].GetToggleMusic().Equals(current))
                {
                    continue;
                }
                else
                {
                    if (setupMusics[i].GetStateToggleMusic() == true)
                    {
                        setupMusics[i].OnClickValueChangePlayMusic(false);
                        setupMusics[i].SetStateToggleMusic(false);
                        setupMusics[i].HideReplayMusic();
                    }
                }
            }
        }
    }
    public bool GetStateRandomToggle()
    {
        return randomMusicTgl.isOn;
    }
}