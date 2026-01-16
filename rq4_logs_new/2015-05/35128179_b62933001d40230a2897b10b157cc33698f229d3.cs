using UnityEngine;
using UnityEngine.UI;
using UnityEngine.EventSystems;
using System.Collections;

public class titleCard : MonoBehaviour {
	private GameObject musicObj;

	void Awake () {
		musicObj = GameObject.Find ("Music");
		musicObj.SetActive (false);
	}
	
	// Update is called once per frame
	void Update () {
		if (Cardboard.SDK.CardboardTriggered) {
			musicObj.SetActive (true);
			musicObj.GetComponent<AudioSource>().Play();
			this.gameObject.SetActive (false);
		}
	}
}