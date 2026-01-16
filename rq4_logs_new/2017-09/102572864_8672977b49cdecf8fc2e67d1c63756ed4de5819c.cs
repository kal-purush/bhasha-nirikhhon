using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class HP : MonoBehaviour {
	public Image image;
	status ST;
	Slider _slider;
	// Use this for initialization
	void Start () {
		ST = image.GetComponent<status> ();
		_slider = this.gameObject.GetComponent<Slider>();
		_slider.maxValue = ST.MAXHP;
	}
	
	// Update is called once per frame
	void Update () {
		ST = image.GetComponent<status> ();
		_slider.value = (float)ST.HP / (float)ST.MAXHP* 100;
	}
}