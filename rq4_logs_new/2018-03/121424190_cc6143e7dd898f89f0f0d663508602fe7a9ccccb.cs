using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.Events;

public class testSpeach : MonoBehaviour {
	string[] actionWords = new string[] {"jump", "hello", "spin"};
	private UnityAction<string> action;


	void Awake(){
		action = new UnityAction<string>(ToTextAction);
	}

	void OnEnable(){
		EventManager.StartListening ("Speech", action);
	}

	void OnDisable(){
		EventManager.StopListening ("Speech", action);
	}

	void ToTextAction(string newText){
		string[] words = newText.Split (' ');
		int jump = 0;
		int hello = 0;
		int spin = 0;
		foreach (string x in words) {
			Debug.Log (x);
			if (x == "jump") {
				jump++;
			} else if (x == "hello") {
				hello++;
			} else if (x == "spin") {
				spin++;
			}
		}
		if (hello > spin && hello > jump)
			Debug.Log( "hello");
		else if (spin > hello && spin > jump)
			Debug.Log ("spin");
		else if (jump > hello && jump > spin)
			Debug.Log( "jump");
		else
			Debug.Log( "null");
	}
}