using UnityEngine;
using System.Collections;
using UnityEngine.UI; 
public class scrollText : MonoBehaviour {
	string myString = "Fuck everything"; 
	string stringAnimate = "";
	public Text textBox; 
	IEnumerator AnimateText(){
		for (int i = 0; i < myString.Length; i++) {
			stringAnimate += myString[i]; 
			yield return new WaitForSeconds(0.07f); 
		}
	}
	
	// Update is called once per frame
	void Update () {
		textBox.text = (stringAnimate);
		if (Input.GetKeyDown (KeyCode.P)) {
			StartCoroutine (AnimateText()); 
		}
	}
}