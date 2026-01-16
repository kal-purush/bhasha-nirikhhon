using UnityEngine;
using System.Collections;

public class FloatingEffect : MonoBehaviour {
    private float time = 0;
	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
        time += Time.deltaTime;
        time = time % ((2/3.5f) * Mathf.PI);
        transform.position = new Vector3(transform.position.x, 0.6f + (0.05f * Mathf.Sin(3.5f*time)), transform.position.z);
	}
}