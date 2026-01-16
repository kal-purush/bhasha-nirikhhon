using System;
using UnityEngine;
using UnityEngine.UI;

public class Settings : Popup
{
	[Header("Sound Controls")]
	public Toggle soundToggle;
	public Slider soundSlider;

	[Header("Music Controls")]
	public Toggle musicToggle;
	public Slider musicSlider;

	private void Start()
	{
		soundToggle.onValueChanged.AddListener(SoundVolumeControl);
		musicToggle.onValueChanged.AddListener(MusicVolumeControl);
		soundSlider.onValueChanged.AddListener(SoundChange);
		musicSlider.onValueChanged.AddListener(MusicChange);
	}

	private void SoundChange(float volume)
	{
		//Добавить PlayerPrefs
		AudioManager.manager.ChangeSoundsVolume(volume);
	}

	private void MusicChange(float volume)
	{
		//Добавить PlayerPrefs
		AudioManager.manager.ChangeMusicVolume(volume);
	}

	public void Test()
	{
		AudioManager.PlaySoundOnce("Test");
	}

	private void SoundVolumeControl(bool value)
	{
		//Добавить PlayerPrefs
		AudioManager.muteSound = value;
		soundSlider.value = value ? 0 : 1;
	}

	private void MusicVolumeControl(bool value)
	{
		//Добавить PlayerPrefs
		AudioManager.muteMusic = value;
		musicSlider.value = value ? 0 : 1;
	}





}