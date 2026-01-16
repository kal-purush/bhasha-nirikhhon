using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DestructorPlataformes : MonoBehaviour {

    public GameObject puntDestruccio;

	// Use this for initialization
	void Start () {
        puntDestruccio = GameObject.Find("PuntDestruccioPlataforma");
	}
	
	// Update is called once per frame
	void Update () {
	    if (transform.position.x < puntDestruccio.transform.position.x)
        {
            //Destroy(gameObject);
            gameObject.SetActive(false);
        }	
	}
}