using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class RotationMidi : MonoBehaviour {
	private float RotatinoSppedX = 0.0f;
	private float RotatinoSppedY = 0.0f;
	// Use this for initialization
	private Vector3 pos ;

	void Awake(){
		pos = new Vector3 (0,0,0);
	}

	public void RotatingX(float newSpeed){
		RotatinoSppedX = newSpeed ;			
	}

	public void RotatingY(float newSpeed){
		RotatinoSppedY = newSpeed ;			
	}

	void Update(){
		pos.y = RotatinoSppedY * Time.deltaTime;
		pos.x = RotatinoSppedX*Time.deltaTime;
		transform.Rotate(pos);
	}

}