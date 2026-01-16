using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CameraZone : MonoBehaviour {

    public Camera playerCam;
    public Camera cam;
    public StillCamScript associatedCamScript;
	
    // Use this for initialization
	void Start ()
    {
		
	}
	
	// Update is called once per frame
	void Update ()
    {
		
	}

    private void OnTriggerEnter(Collider collision)
    {
        Debug.Log("Entered collider");
        if (collision.gameObject.tag == "Player")
        {
            playerCam.enabled = false;
            associatedCamScript.ActivateCam();
        }
    }
    
    private void OnTriggerExit(Collider collision)
    {
        Debug.Log("Left collider");
        if (collision.gameObject.tag == "Player")
        {
            playerCam.enabled = true;
            associatedCamScript.DeactivateCam();
        }
    }
}