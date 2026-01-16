using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CameraController : MonoBehaviour {

	private GameObject Player;

	void Start()
	{
		Player = GameObject.Find ("Player");
	}
	
	// Update is called once per frame
	void Update () 
	{
		transform.position = new Vector3 (Player.transform.position.x, 15, Player.transform.position.z);
	}
}