using UnityEngine;
using System.Collections;

public class PlasticFeatures : MonoBehaviour {

	//GameObject plastic;
	GameObject anatomy;
	float deathSequence;

	// Use this for initialization
	void Start () 
	{
		//plastic = GameObject.FindGameObjectWithTag ("Plastic");
		//anatomy = GameObject.FindGameObjectWithTag ("Anatomy");
		deathSequence = 3;
	}
	
	// Update is called once per frame
	void Update () 
	{
		anatomy = GameObject.FindGameObjectWithTag ("Anatomy");
	}

	void OnTriggerEnter (Collider other) 
	{
		if(other.gameObject == anatomy)
		{
			print ("collided with anatomy");
			StartCoroutine (DeathSequence());
		}
	}

	IEnumerator DeathSequence()
	{
		yield return new WaitForSeconds (deathSequence);
		gameObject.SetActive (false);
	}
}