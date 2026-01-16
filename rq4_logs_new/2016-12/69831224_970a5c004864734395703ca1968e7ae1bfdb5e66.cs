using UnityEngine;
using System.Collections;

public class Cinematics : MonoBehaviour {

    [SerializeField]
    Camera cameraCinematic;
    [SerializeField]
    Camera mainCamera;
    
	// Use this for initialization
	void Start () {

        GameObject.Find("Bat").GetComponent<BatController>().enabled = false;
        mainCamera.GetComponent<Camera>().enabled = false;
        cameraCinematic.GetComponent<Camera>().enabled = true;
        //Play animation from the cinematic camera: 
        cameraCinematic.GetComponent<Animation>().Play();


    }

    public void switchCamera()
    {
        mainCamera.GetComponent<Camera>().enabled = true;
        cameraCinematic.GetComponent<Camera>().enabled = false;
        GameObject.Find("Bat").GetComponent<BatController>().enabled = true;
    }
}