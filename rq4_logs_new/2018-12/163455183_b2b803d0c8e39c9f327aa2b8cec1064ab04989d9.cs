using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class melee : MonoBehaviour {
    Animator manim;
	// Use this for initialization
	void Start ()
    {
        manim = GameObject.FindWithTag("Player").GetComponent<Animator>();
    }
	
	// Update is called once per frame
	void Update ()
    {
		if(Input.GetMouseButtonDown(0))
        {
            manim.SetTrigger("hit");
        }
	}
}