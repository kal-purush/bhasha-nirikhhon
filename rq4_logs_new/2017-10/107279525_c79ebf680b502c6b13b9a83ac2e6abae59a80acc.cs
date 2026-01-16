using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ExpandBullet : BulletController {

	void Start () {
		
	}

	void Update () {
		GetComponent<Transform> ().localScale *= 1.1f;
	}
}