using UnityEngine;
using System.Collections;

public class DeathEffect : MonoBehaviour {

	public GameObject deathEffect;

	private bool isQuitting = false;

	void OnApplicationQuit () {

		isQuitting = true;

	}

	void OnDestroy () {

		if (!isQuitting) {

			GameObject newDeathEffect = Instantiate (deathEffect, transform.position, Quaternion.identity) as GameObject;

		}

	}

}