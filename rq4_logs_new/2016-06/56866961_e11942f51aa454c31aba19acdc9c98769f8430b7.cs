using UnityEngine;
using System.Collections;

public class Level2 : MonoBehaviour {

	void OnCollisionEnter() {
		Collider();
	}
	void Collider() {
		Application.LoadLevel("Level2");
	}
}