using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class StartSceneButtonController : MonoBehaviour {

	public GameObject bagel, run;
	private float currentRemainTime;
	private SpriteRenderer spRenderer;
	public float fadeTime = 1f;

	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
//		float alpha = currentRemainTime / fadeTime;
//		SpriteRenderer color = spRenderer.color;
//		color.a = alpha;
//		spRenderer.color = color;
//		bagel.GetComponent<SpriteRenderer> ().color = new Color(1.0f, 1.0f, 1.0f, color);
//		run.GetComponent<SpriteRenderer> ().color = new Color(1.0f, 1.0f, 1.0f, color);
	}

	public void OnClick() { // MUST public
		PlayerPrefs.SetInt("StartButton", 1);
		bagel.gameObject.SetActive (false);
		run.gameObject.SetActive (false);
		print ("clicked");
	}
}