using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Collision : MonoBehaviour {

	// reference na glavnu skriptu
	public Controller glavnaSkripta;

	public AudioClip hitClip;
	AudioSource source;

	// Use this for initialization
	void Start () {
		source = GetComponent<AudioSource>();		
	}
	
	// Update is called once per frame
	void Update () {
		
	}

	// IEnumerator je potreban zbog yield-a
	IEnumerator OnTriggerEnter(Collider drugiObjekt) {
		source.PlayOneShot(hitClip);
		glavnaSkripta.PovecajBodove();

		// upali i nakon pola sekunde ugasi
		GetComponent<Light>().enabled = true;
		yield return new WaitForSeconds (0.5f);
		GetComponent<Light>().enabled = false;
	}
}