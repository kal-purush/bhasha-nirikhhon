using UnityEngine;
using System.Collections;

public class DemoIntegrationTest : MonoBehaviour {

	//private float _elapsedTime = 0.0f;

	private bool _isActive = false;

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	
		if (_isActive) {
			//_elapsedTime += Time.deltaTime;
		}

	}

	void OnTriggerEnter(Collider other)
	{
		if (_isActive) {
			//Testing.
		}
	}
}