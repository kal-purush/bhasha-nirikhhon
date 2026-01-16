using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class TweakMegaNoise : MonoBehaviour {
	private MegaNoise megaNoise;
	private Vector3 megaStrength;
	private float freq;
	private float scale;
	// Use this for initialization
	void Start () {
		megaStrength = new Vector3 (0, 0, 0);
		freq = 0.25f;
		megaNoise = GetComponent<MegaNoise> ();
	}


	public void ChangeMegaNoiseX(float x){
		megaStrength.x = x;
	}
	public void ChangeMegaNoiseY(float y){
		megaStrength.y = y;
	}
	public void ChangeMegaNoiseZ(float z){
		megaStrength.z = z;
	}

	public void ChangeMegaNoiseScale(float s){
		scale = s;
	}
		
	public void ChangeMegaNoiseFreq(float f){
		freq = f;
	}

	// Update is called once per frame
	void Update () {
		megaNoise.Strength = megaStrength;
		megaNoise.Scale = 1.0f-scale;
		megaNoise.Freq = freq;
	}
}