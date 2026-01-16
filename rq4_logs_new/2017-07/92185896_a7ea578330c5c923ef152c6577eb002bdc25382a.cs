using System.Collections;
using System.Collections.Generic;
using UnityEngine.UI;
using UnityEngine;

public class Settings : MonoBehaviour {
	private GameController control;
	public Slider BGMSlider;
	public Slider SFXSlider;
	public Toggle gameMode;

	// Use this for initialization
	void Start () {
		control = FindObjectOfType<GameController>();
		BGMSlider.normalizedValue = control.getBGMVol();
		SFXSlider.normalizedValue = control.getSFXVol();
		gameMode.isOn = control.getGameMode();
	}
	
	void Update() {
		control.setVolume("BGM", BGMSlider.normalizedValue);
		control.setVolume("SFX", SFXSlider.normalizedValue);
		
		control.setGameMode(gameMode.isOn);
	}
	
	void changeGameMode(){
		
	}
}