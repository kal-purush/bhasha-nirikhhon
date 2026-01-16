using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using System;

public class Clock : MonoBehaviour {
	Timer time;
	public Text cText;
	// Use this for initialization
	void Start () {
		time = FindObjectOfType<Timer>();
		cText = FindObjectOfType<Text>();
	}
	
	// Update is called once per frame
	void Update () {
		double min = Math.Floor(time.getTime()/60);
		double sec = time.getTime() - min*60;
		
		if(sec < 10){
			cText.text = min.ToString() + ":0" + sec.ToString();
		} else {
			cText.text = min.ToString() + ":" + sec.ToString();
		}
	}
}