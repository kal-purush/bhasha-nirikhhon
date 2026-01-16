using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Pulse : MonoBehaviour {
    Material mat;
	// Use this for initialization
	void Start () {
        mat = GetComponent<Renderer>().material;
	}
	
	// Update is called once per frame
	void Update () {
        float start = 0.1f, end = 0.05f;
        Color emissioncolor = (new Color(0.6617647f, 0.9440162f, 1)) *  Mathf.LinearToGammaSpace((start+end)/2 + (start-end)/2*Mathf.Sin(5*Time.time));
        //Debug.Log(emissioncolor.r.ToString() + " " + emissioncolor.g.ToString() + " " + emissioncolor.b.ToString());
        mat.SetColor("_EmissionColor", emissioncolor);
        Debug.Log(mat.GetColor("_EmissionColor"));
	}
}