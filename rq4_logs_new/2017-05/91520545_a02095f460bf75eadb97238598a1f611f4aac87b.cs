using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System;

public class InternetChecker : SingletonMonoBehaviour<InternetChecker> {

	IEnumerator CheckInternetConnection(Action<bool> action, string url){
		WWW www = new WWW(url);
		yield return www;
		if (www.error != null) {
			action (false);
		} else {
			action (true);
		}
	} 

	public void CheckConnection(Action<bool> isConnected, string url){
		StartCoroutine(CheckInternetConnection(isConnected, url));
	}


}