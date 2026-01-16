using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Ready : MonoBehaviour {

	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
        SpriteRenderer sr = this.GetComponent<SpriteRenderer>();
        Color color= sr.color;
        color.a = Mathf.Max(0f, color.a - 0.005f);
        sr.color = color;
	}
}