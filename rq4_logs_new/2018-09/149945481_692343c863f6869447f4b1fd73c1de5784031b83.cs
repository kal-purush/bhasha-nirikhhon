using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class LightToggle : MonoBehaviour {
	
	[SerializeField]
	public DayController daylight;

	private int nombreDeJoueursSurLeBouton = 0;
	private float tempsRestant = 2;

	void OnTriggerExit2D() {
		nombreDeJoueursSurLeBouton--;
		if (nombreDeJoueursSurLeBouton == 0) {
			tempsRestant = 2;
		}
		Debug.Log(nombreDeJoueursSurLeBouton);
	}

	void OnTriggerEnter2D() {
		nombreDeJoueursSurLeBouton++;
		Debug.Log(nombreDeJoueursSurLeBouton);
	}

	void Update() {
		if (nombreDeJoueursSurLeBouton > 0) {
			tempsRestant -= Time.deltaTime;
			Debug.Log(tempsRestant);
			if (tempsRestant <= 0) {
				daylight.Toggle();
				Destroy(gameObject);
			}
		}
	}
}