using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Trigger : MonoBehaviour {

	private PlayerAnalog pAS;
	private EnemyStats es;

	private GameObject Gabumon; 
	
	private float LDamagePerTick = .05f; 

	void Start(){
		Gabumon = GameObject.Find("Gabumon"); 
		es = Gabumon.GetComponent<EnemyStats>(); 
	}
	
	void OnTriggerEnter2D (Collider2D col){
		if (col.gameObject.tag == "PlayerAnalog"){
			pAS = col.GetComponent<PlayerAnalog>();
			pAS.TurnPlayerRed();
		}
	}
	
	void OnTriggerStay2D (Collider2D col){
		es.LDamage(LDamagePerTick); 
	}
	
	void OnTriggerExit2D (Collider2D col){
		if (col.gameObject.tag == "PlayerAnalog"){
			pAS = col.GetComponent<PlayerAnalog>();
			pAS.TurnPlayerWhite();
		}		
	}

}