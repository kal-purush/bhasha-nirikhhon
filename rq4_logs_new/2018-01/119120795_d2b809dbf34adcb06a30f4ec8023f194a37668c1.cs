using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class camControl : MonoBehaviour {

	Vector3 targetPos;
	public float dampSpeed;
	public GameObject Fish1;
	public GameObject Fish2;
	Vector3 velocity =  Vector3.zero;


	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
		
		targetPos = new Vector3((Fish1.transform.position.x + Fish2.transform.position.x) / 2f, (Fish1.transform.position.y + Fish2.transform.position.y) / 2f, -10f);
		print(targetPos);
		//transform.position = Vector3.SmoothDamp(transform.position, targetPos, ref velocity, dampSpeed) * Time.deltaTime;
		transform.position = Vector3.Lerp(transform.position, targetPos, 0.5f);
		
	}
}