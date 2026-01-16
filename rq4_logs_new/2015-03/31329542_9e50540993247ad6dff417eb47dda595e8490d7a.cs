using UnityEngine;
using System.Collections;

public class cameraControl : MonoBehaviour {

	public GameObject playercube;
	public GameObject maincamera;
	public int cameraDist = 10;


	// Use this for initialization
	void Start () {
		playercube = GameObject.Find ("playercube");
		maincamera = GameObject.Find ("Main Camera");
	
	}
	
	// Update is called once per frame
	void Update () {
		//maincamera.transform.position = playercube.transform.position;

	
	}
}