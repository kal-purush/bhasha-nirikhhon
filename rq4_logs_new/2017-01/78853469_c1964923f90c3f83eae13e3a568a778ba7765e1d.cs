using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CrystalOfSnowGenerator : MonoBehaviour {

	[SerializeField]
	GameObject snowPrefab;

	float span = 1.0f;
	float delta = 0f;

	// Update is called once per frame
	void Update () {

		this.delta += Time.deltaTime;
		if (this.delta > this.span) {
			this.delta = 0;
			GameObject obj = Instantiate (snowPrefab) as GameObject;
			int px = Random.Range (-20, 50);
			int pz = Random.Range (-5, 5);
			obj.transform.position = new Vector3 (px, 20f, pz);
		}
	}
}