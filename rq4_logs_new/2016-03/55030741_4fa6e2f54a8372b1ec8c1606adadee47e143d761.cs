using UnityEngine;
using System.Collections;

public class IAMove : MonoBehaviour {

	GameObject bola;
	GameObject paleta;

	// Use this for initialization
	void Start () {
		bola = GameObject.FindGameObjectWithTag("Bola");
		paleta = GameObject.FindGameObjectWithTag("PaletaIA");
	}
	
	// Update is called once per frame
	void Update ()  {
		if(paleta.transform.position.x>-10 && paleta.transform.position.x<10 && Mathf.Abs(paleta.transform.position.z-bola.transform.position.z) < 20)
        {
			paleta.transform.position = new Vector3(bola.transform.position.x, 0.8f, 18.0f);
		}
	}
}