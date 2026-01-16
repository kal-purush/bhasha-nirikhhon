using UnityEngine;
using System.Collections;

public class Carro : MonoBehaviour {

	public GameObject carro;
	public Camera cam;
	private Vector3 screen;



	// Use this for initialization
	void Start () {

	}
	
	// Update is called once per frame
	void Update () {
		gameObject.transform.position = new Vector3 (gameObject.transform.position.x + 0.35f, gameObject.transform.position.y, gameObject.transform.position.z);
		Remover ();
	}

	private void Remover(){
		if ( gameObject.transform.position.x > 168.8f) {
			Destroy (gameObject);
		} 
	} 
}