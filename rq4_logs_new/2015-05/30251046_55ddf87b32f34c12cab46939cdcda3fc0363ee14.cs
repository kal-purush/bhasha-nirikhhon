using UnityEngine;
using System.Collections;

public class Surgeon : MonoBehaviour {

	//GameObject plastic;
	public Inventory inventory;
	GameObject poisonhead;
	float deathSequence;
	
	// Use this for initialization
	void Start () 
	{
		inventory = GameObject.FindGameObjectWithTag ("Player").GetComponent<Inventory>();
		poisonhead = GameObject.FindGameObjectWithTag ("PoisonHead");
		deathSequence = 3;
	}
	
	// Update is called once per frame
	void Update () 
	{
		
	}
	
	void OnTriggerEnter (Collider other) 
	{
		if(other.gameObject == poisonhead)
		{
			print ("collided with poisonhead");
			StartCoroutine (DeathSequence());
		}
	}
	
	IEnumerator DeathSequence()
	{
		inventory.activePoisonHead = false;
		inventory.holdingPoisonHead.SetActive(false);
		inventory.poisonheadSwap = false;
		yield return new WaitForSeconds (deathSequence);
		gameObject.SetActive (false);
	}
}