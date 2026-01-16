using UnityEngine;
using System.Collections;

public class ReplacePlayer : MonoBehaviour {

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	
	}
	
	void OnTriggerEnter(Collider other) {
		if (other.transform.tag == "Player") 
		{
			other.transform.position = other.transform.GetComponent<PlayerController>().startPos;
		}
	}
}