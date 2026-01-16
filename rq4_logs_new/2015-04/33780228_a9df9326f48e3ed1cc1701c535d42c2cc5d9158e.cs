using UnityEngine;
using System.Collections;

public class Shake : MonoBehaviour {

	public float intensity = 10f;

	private Vector3 position;

	// Use this for initialization
	void Start () {
		position = transform.position;
	}
	
	// Update is called once per frame
	void Update () {
		Vector2 random = Random.insideUnitCircle * intensity;
		transform.position = position + new Vector3(random.x, random.y, 0f);
	}
}