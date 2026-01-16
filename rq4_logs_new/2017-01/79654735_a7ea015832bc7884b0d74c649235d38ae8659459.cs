using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class RockGenerator : MonoBehaviour {
	// Inner radius, rocks do not appear inside this circle.
	public float InnerRadius;
	// Outer radius, rocks appear inside this circle.
	public float OuterRadius;
	// Object to spawn as rocks.
	public GameObject Rock;

	void Start() {
		this.Rock.SetActive(false);
		this.Generate();
	}

	void Generate() {
		foreach (Transform child in this.transform) {
			Object.Destroy(child.gameObject);
		}
		for (int i = 0; i < 10; i++) {
			float x = Random.Range(-1.0f, +1.0f) * this.OuterRadius;
			float y = Random.Range(-1.0f, +1.0f) * this.OuterRadius;
			float r2 = x * x + y * y;
			Debug.Log(string.Format("{0} {1} {2} {3}", x, y, this.InnerRadius, this.OuterRadius));
			if (r2 < this.InnerRadius * this.InnerRadius) {
				Debug.Log("Skipped");
				continue;
			}
			GameObject rock = Object.Instantiate(this.Rock, new Vector3(x, y), Quaternion.identity, this.transform);
			rock.SetActive(true);
		}
	}
}