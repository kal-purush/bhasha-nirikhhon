using UnityEngine;
using System.Collections;

public class ScoreController : MonoBehaviour {
	//public GameObject resultStar;
	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	}

	public void GetStar() {
		print ("getStar");
		transform.Rotate(0.0f, 0.0f,0.1f*Time.deltaTime);
	}
}