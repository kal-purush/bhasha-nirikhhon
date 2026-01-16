using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GameController : MonoBehaviour {

	public GameObject plugPrefab;
	public float colSpace;
	public float rowSpace;

	// Use this for initialization
	void Start () {
		InstantiatePlugs ();
	}

	private void InstantiatePlugs() {
		for (var col = 0 ; col < 4; ++col) {
			for (var row = 0; row < 4; ++row) {
				var position = new Vector3(-3.5f + colSpace * col, 3.5f + rowSpace * -1 * row, 0f);		
				Instantiate(plugPrefab, position, Quaternion.Euler(90, 0, 0));
			}
		}
	}
	
	// Update is called once per frame
	void Update () {
		
	}
}