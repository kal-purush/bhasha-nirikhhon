using UnityEngine;
using System.Collections;

public class BlackPLaneBehaviour : MonoBehaviour {

	private bool initialFade = true;
	private float framesCounter = 4;

	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
		if (initialFade) {
			framesCounter -= Time.deltaTime * 2;
			transform.GetComponent<Renderer> ().material.color = new Color (0, 0, 0, framesCounter);
			if(transform.GetComponent<Renderer> ().material.color.a <= 0) initialFade = false;
		}
	}
}