using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class TestAnim : MonoBehaviour {

	Animator anim;

	void Start()
	{
		anim = GetComponent<Animator> ();
	}

	void Update()
	{
		if (Input.GetKeyDown(KeyCode.Space))
		{
			Debug.Log("Playing animation");
			anim.SetTrigger("SpacePressed");
		}
	}
}