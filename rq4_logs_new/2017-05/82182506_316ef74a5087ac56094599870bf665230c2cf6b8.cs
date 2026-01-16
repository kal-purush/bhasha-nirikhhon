using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CreditsScript : MonoBehaviour {

    public Transform mainCanvas;
    public Transform creditsCanvas;

	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
        if (Input.GetKeyDown("space"))
        {
            mainCanvas.gameObject.SetActive(true);
            creditsCanvas.gameObject.SetActive(false);
        }


    }
}