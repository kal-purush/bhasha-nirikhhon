using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class TimeSlider : MonoBehaviour {

	[SerializeField]
	private static UnityEngine.UI.Slider timeSlider;
	private static Graph graph = new Graph();

	void Awake() {
		GameObject temp = GameObject.Find("Slider Time");
		timeSlider = temp.GetComponent<UnityEngine.UI.Slider> ();
	}

	public static void setMinMax(int min, int max){
		timeSlider.minValue = min;
		timeSlider.maxValue = max;
	}
	public static int getValue(){
		int val = (int)timeSlider.value;
		return  val;
	} 
	public static void setValue(){
		timeSlider.value = 0;
	} 


}