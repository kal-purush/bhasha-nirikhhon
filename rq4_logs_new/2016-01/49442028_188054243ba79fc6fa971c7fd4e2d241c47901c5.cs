using UnityEngine;
using System.Collections;
using System.Collections.Generic;
public class instantiation : MonoBehaviour {
	public Transform wallPrefab; 
	public float compareValueX; 
	private Vector3 instantiatePos; 
	bool canIGo; 

	// Use this for initialization
	void Start () {
		canIGo = true; 
		instantiatePos = transform.position; 
	}
	
	// Update is called once per frame
	void Update () {
		if (wallPrefab.position.x <= compareValueX && canIGo){
			Instantiate(wallPrefab, instantiatePos, wallPrefab.localRotation);  
			canIGo = false; 
			//Destroy (this.gameObject); 
		}

	}

}