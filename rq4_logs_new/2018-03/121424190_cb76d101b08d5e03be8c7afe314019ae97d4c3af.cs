using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.UI;

public class Menu : MonoBehaviour {
	//change scene
	public void changeScene(string loadScene){
		SceneManager.LoadScene(loadScene);
	}

	public void Start(){
		GameObject levelObj = GameObject.Find ("level");
		GameObject sliderObj = GameObject.Find("xpSlider");

		if (levelObj != null && sliderObj != null) {
			//set level text from memory
			Text levelText = levelObj.GetComponent<Text> ();
			levelText.GetComponentInChildren<Text>().text = ""+PlayerPrefs.GetInt ("level");

			//set xp slider fullness
			Slider xpSlider = sliderObj.GetComponent<Slider>();
			xpSlider.value = PlayerPrefs.GetFloat ("xp");
		}
	
	}
}