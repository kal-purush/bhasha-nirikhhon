using UnityEngine;
using System.Collections;

public class ShadowManager : MonoBehaviour {

	private float startY;
	// The rate the goo rises, in units per second
	public float riseRate = 0.5f;

	// Use this for initialization
	void Start () {
		//GameObject o = GameObject.Find("Floor");
		//transform.localScale = new Vector3(o.transform.lossyScale.x, 1f, o.transform.lossyScale.z);
		startY = transform.position.y;
	}
	
	// Update is called once per frame
	void Update () {
		// Slowly rise
		transform.localScale += new Vector3(0f, riseRate * Time.deltaTime, 0f);
		transform.position = new Vector3(transform.position.x, startY + (transform.localScale.y/2), transform.position.z);
	}
}