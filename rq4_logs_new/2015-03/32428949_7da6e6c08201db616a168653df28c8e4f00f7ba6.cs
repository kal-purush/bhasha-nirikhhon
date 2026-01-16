using UnityEngine;
using System.Collections;

public class ToLevels : MonoBehaviour {
	
	void Start () {
	}



	public void OnMouseDown() {
		if (Application.loadedLevel == 1) {
			Application.LoadLevel(2);
		}
		if (Application.loadedLevel == 2) {
			Application.LoadLevel(3);
		}
	}
	
	

	// Update is called once per frame
	void Update () {
		
	}
}