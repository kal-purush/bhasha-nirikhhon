using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PowerManager : MonoBehaviour {

	public int goatDamage;
	public int dragonDamage;

	// Use this for initialization
	void Start () {


	}
	
	// Update is called once per frame
	void Update () {
		
	}

	//intensifies low power for goat
	void lowPowerIntensity () {
	
		if ((GetComponent<StateTracker> ().lowStateConsecutive) == 1) {
			goatDamage = Random.Range (10, 15);
			GetComponent<StateTracker> ().myFlowchart.SetFloatVariable ("offenseDamageCurrent", goatDamage);
			Debug.Log ("Goat Weak 1");
		} else if ((GetComponent<StateTracker> ().lowStateConsecutive) == 2) {
			goatDamage = Random.Range (15, 20);
			GetComponent<StateTracker> ().myFlowchart.SetFloatVariable ("offenseDamageCurrent", goatDamage);
			Debug.Log ("Goat Weak 2");
		}else if ((GetComponent<StateTracker> ().lowStateConsecutive) >= 3) {
			goatDamage = Random.Range (20, 25);
			GetComponent<StateTracker> ().myFlowchart.SetFloatVariable ("offenseDamageCurrent", goatDamage);
			Debug.Log ("Goat Weak 3");
		}
	}


	//weakens low power for dragon
	void lowPowerWeakened () {

		if ((GetComponent<StateTracker> ().lowStateConsecutive) == 1) {
			//GetComponent<StateTracker>().myFlowchart.
			dragonDamage = Random.Range (3, 5);
			GetComponent<StateTracker> ().myFlowchart.SetFloatVariable ("offenseDamageCurrent", dragonDamage);
		} else if ((GetComponent<StateTracker> ().lowStateConsecutive) == 2) {
			dragonDamage = Random.Range (3, 1);
			GetComponent<StateTracker> ().myFlowchart.SetFloatVariable ("offenseDamageCurrent", dragonDamage);
		}else if ((GetComponent<StateTracker> ().lowStateConsecutive) >= 3) {
			dragonDamage = Random.Range (0, 1);
			GetComponent<StateTracker> ().myFlowchart.SetFloatVariable ("offenseDamageCurrent", dragonDamage);
		}
	}



	//intensifies high power for dragon
	void hiPowerIntensity () {

		if ((GetComponent<StateTracker> ().hiStateConsecutive) == 1) {
			dragonDamage = Random.Range (10, 15);
			GetComponent<StateTracker> ().myFlowchart.SetFloatVariable ("offenseDamageCurrent", dragonDamage);
			Debug.Log ("Dragon Weak 1");
		} else if ((GetComponent<StateTracker> ().hiStateConsecutive) == 2) {
			dragonDamage = Random.Range (15, 20);
			GetComponent<StateTracker> ().myFlowchart.SetFloatVariable ("offenseDamageCurrent", dragonDamage);
			Debug.Log ("Dragon Weak 2");
		}else if ((GetComponent<StateTracker> ().hiStateConsecutive) >= 3) {
			dragonDamage = Random.Range (20, 25);
			GetComponent<StateTracker> ().myFlowchart.SetFloatVariable ("offenseDamageCurrent", dragonDamage);
			Debug.Log ("Dragon Weak 3");
		}
	}

	//weakens high power for goat
	void hiPowerWeakened () {

		if ((GetComponent<StateTracker> ().hiStateConsecutive) == 1) {
			//GetComponent<StateTracker>().myFlowchart.
			dragonDamage = Random.Range (3, 5);
			GetComponent<StateTracker> ().myFlowchart.SetFloatVariable ("offenseDamageCurrent", goatDamage);
		} else if ((GetComponent<StateTracker> ().hiStateConsecutive) == 2) {
			dragonDamage = Random.Range (1, 3);
			GetComponent<StateTracker> ().myFlowchart.SetFloatVariable ("offenseDamageCurrent", goatDamage);
		}else if ((GetComponent<StateTracker> ().hiStateConsecutive) >= 3) {
			dragonDamage = Random.Range (0, 1);
			GetComponent<StateTracker> ().myFlowchart.SetFloatVariable ("offenseDamageCurrent", goatDamage);
		}
	}
}