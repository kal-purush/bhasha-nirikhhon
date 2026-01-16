using UnityEngine;
using System.Collections;

public class Jumper : MonoBehaviour {

	//GameObject plastic;
	GameObject gloves;
	float deathSequence;
	
	// Use this for initialization
	void Start () 
	{
		//plastic = GameObject.FindGameObjectWithTag ("Plastic");
		gloves = GameObject.FindGameObjectWithTag ("Gloves");
		deathSequence = 3;
	}
	
	// Update is called once per frame
	void Update () 
	{
		
	}
	
	void OnTriggerEnter (Collider other) 
	{
		if(other.gameObject == gloves)
		{
			print ("collided with gloves");
			StartCoroutine (DeathSequence());
		}
	}
	
	IEnumerator DeathSequence()
	{
		yield return new WaitForSeconds (deathSequence);
		gameObject.SetActive (false);
	}
}