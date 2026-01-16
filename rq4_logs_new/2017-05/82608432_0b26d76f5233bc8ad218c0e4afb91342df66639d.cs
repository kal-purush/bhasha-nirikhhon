using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Blink : MonoBehaviour {

	Vector3 idleScale;
	Animator blinkAnimator;
	// Use this for initialization
	void Start () {
		idleScale = transform.localScale;
		blinkAnimator = transform.parent.GetComponent<Animator> ();
		StartCoroutine (GenerateBlink());
	}
	
	// Update is called once per frame
	void Update () {
		
	}

	IEnumerator GenerateBlink()
	{
		while (true) {
			int blinkType = (int)(Mathf.Floor (Random.Range (1.01f, 2.99f)));
			float seconds = Random.Range (0.5f, 1.0f);
			yield return new WaitForSeconds(seconds);
			if (blinkType == 1) {
				blinkAnimator.SetBool ("blink", true);
			} else {
				blinkAnimator.SetBool ("blinkTwice", true);
			}
			yield return new WaitForSeconds (0.2f);
			if (blinkType == 1) {
				blinkAnimator.SetBool ("blink", false);
			} else {
				blinkAnimator.SetBool ("blinkTwice", false);
			}

				

		}
	}
}