using UnityEngine;
using UnityEngine.UI;
using System.Collections;
using System.Collections.Generic;

public class StationDropdown : MonoBehaviour {
	public Dropdown menu;
	public GameObject station;

	public bool hasDish = false;
	List<string> dropDownOptions = new List<string>();
	// Use this for initialization
	void Start () {
		menu = GetComponent<Dropdown> ();
		transform.gameObject.SetActive (false);
		dropDownOptions.Add ("");
		dropDownOptions.Add ("PutDown");
		dropDownOptions.Add ("PickUp");
		dropDownOptions.Add ("AddTo");
		ChangeOptions (false);
	}

	string recentValue;
	public void UpdateRecentValue(){
		recentValue = menu.options[menu.value].text;
	}

	public void CheckSelectedBox(){
		if (!hasDish) {
			if (menu.value == 1) {
				PutDown ();
			} else if (menu.value == 2) {
				PickUp ();
			} else if (menu.value == 3) {
				AddTo ();
			} else if (menu.value == 0) {
				station.GetComponent<Station> ().Cancel ();
			} else {
				station.GetComponent<Station> ().Cancel ();
			}
		} else {			
			if (menu.value == 1) {
				PickUp ();
			} else if (menu.value == 2) {
				AddTo ();
			} else if (menu.value == 0) {
				station.GetComponent<Station> ().Cancel ();
			} else {				
				Debug.Log (recentValue);
				if (recentValue != null && station.GetComponent<Station> ().dish != null) {
					Debug.Log ("Boop");
					if (CookingController.Instance.SupportsAction (station.GetComponent<Station> ().dish.GetComponent<Ingredients> ().primaryIngredientName, recentValue)) {
						Debug.Log ("Bing");					
						station.GetComponent<Station> ().Cook (menu.options [menu.value].text);
					}
				}
			}
		}
	}

	public void ChangeOptions(bool dish){
		hasDish = dish;
		menu.ClearOptions ();
		if (dish) {
			string[] processes = CookingController.Instance.GetProcessesForStation (station.name);
			List<string> processList = new List<string>();
			processList.Add ("");
			processList.Add ("PickUp");
			processList.Add ("AddTo");
			for (int i = 0; i < processes.Length; i++){
				processList.Add (processes [i]);
			}
			menu.AddOptions (processList);
		} else {
			menu.AddOptions (dropDownOptions);
		}
	}

	public void AddTo(){
		Debug.Log ("AddTo");
		if (station.GetComponent<Station> ().dish != null) {
			station.GetComponent<Station> ().AddTo ();
		} else {
			station.GetComponent<Station> ().Cancel ();
		}
	}

	public void PickUp(){
		Debug.Log ("PickUp");
		if (station.GetComponent<Station> ().dish != null) {
			station.GetComponent<Station> ().PickUp ();
		} else {
			station.GetComponent<Station> ().Cancel ();
		}
	}

	public void PutDown(){
		Debug.Log ("PutDown");
		if (station.GetComponent<Station> ().dish == null) {
			station.GetComponent<Station> ().PutDown ();
		} else {
			station.GetComponent<Station> ().Cancel ();
		}
	}
}