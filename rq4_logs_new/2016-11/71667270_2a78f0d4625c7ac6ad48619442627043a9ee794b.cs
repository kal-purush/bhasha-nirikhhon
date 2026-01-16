using UnityEngine;
using System.Collections;

public class Character : MonoBehaviour {
		
	public int health = 10;

	void Update() {
		if (health == 0) {
			Destroy (gameObject);
			Debug.Log("Game Over.....");
		}
	}
}
