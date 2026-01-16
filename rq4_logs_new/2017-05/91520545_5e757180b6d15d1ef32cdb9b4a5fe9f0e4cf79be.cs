using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System;

public class EffectManager : SingletonMonoBehaviour<EffectManager> {

	public GameObject loadingScreen;
	public GameObject matchingScreen;


	public void StartLoadingScreen(Action action){
		loadingScreen.gameObject.SetActive(true);
		action ();
	
	}

	public void StopLoadingScreen(){
		loadingScreen.gameObject.SetActive(false);
	
	}



	public void StartMatchingScreen(){
		matchingScreen.gameObject.SetActive(true);

	}

	public void StopMatchingScreen(){
		matchingScreen.gameObject.SetActive(false);

	}


}