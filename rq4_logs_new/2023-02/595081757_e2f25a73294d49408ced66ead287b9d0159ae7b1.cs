using GameConstants;
using UnityEngine;
using UnityEngine.Audio;
using UnityEngine.UI;

namespace Audio
{
    public class VolumeSettings : MonoBehaviour
    {
        [SerializeField] private AudioMixer masterMixer;
        [SerializeField] private Slider masterSlider, musicSlider, sfxSlider, dialogueSlider, ambientSlider;

        private void Awake()
        {
            masterSlider.onValueChanged.AddListener(SetMasterVolume);
            musicSlider.onValueChanged.AddListener(SetMusicVolume);
            sfxSlider.onValueChanged.AddListener(SetSfxVolume);
            dialogueSlider.onValueChanged.AddListener(SetDialogueVolume);
            ambientSlider.onValueChanged.AddListener(SetAmbientVolume);
        }

        public void SetMasterVolume(float value)
        {
            masterMixer.SetFloat(Strings.MasterVol, Mathf.Log10(value) * 20);
            Debug.Log("Settings master vol = " + value);
        }
        public void SetMusicVolume(float value)
        {
            masterMixer.SetFloat(Strings.MusicVol, Mathf.Log10(value) * 20);
        }
        public void SetSfxVolume(float value)
        {
            masterMixer.SetFloat(Strings.SfxVol, Mathf.Log10(value) * 20);
        }
        public void SetDialogueVolume(float value)
        {
            masterMixer.SetFloat(Strings.DialogueVol, Mathf.Log10(value) * 20);
        }
        public void SetAmbientVolume(float value)
        {
            masterMixer.SetFloat(Strings.AmbientVol, Mathf.Log10(value) * 20);
        }
    }
}