using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerControl : MonoBehaviour {
	public float sped;



	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
		if (Input.GetKey("w")){
			transform.Translate (Vector3.up*50*sped);
		}
		if (Input.GetKey("s")){
			transform.Translate (Vector3.down*30*sped);
		}
		if (Input.GetKey("a")){
			transform.Translate (Vector3.left*20*sped);
		}
		if (Input.GetKey("d")){
			transform.Translate (Vector3.right*10*sped);
		}




	}
}