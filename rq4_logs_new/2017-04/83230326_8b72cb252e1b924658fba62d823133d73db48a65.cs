using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.UI;

public class ReturnToMenu : MonoBehaviour {

	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
        if (Input.GetButtonDown("Fire1"))
        {
            Image leftArrowLit = transform.GetComponent<Image>();
            leftArrowLit.enabled = true;
        }
        else if (Input.GetButtonUp("Fire1"))
        {
            SceneManager.LoadScene("StartScreen", LoadSceneMode.Single);
        }
	}
}