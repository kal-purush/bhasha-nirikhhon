using UnityEngine;
using System.Collections;
using UnityEngine.Networking;

public class Player_NetworkSetup : NetworkBehaviour {

    Movement mov;
    Weapons weapons;

    Camera cam;

	// Use this for initialization
	void Start () {
	    
        if (isLocalPlayer)
        {
            mov = GetComponent<Movement>();
            mov.enabled = true;

            weapons = GetComponent<Weapons>();
            weapons.enabled = true;

            cam = GetComponent<Camera>();

            cam.GetComponent<Camera>().enabled = true;
            cam.gameObject.SetActive(true);

            cam.GetComponent<AudioListener>().enabled = true;
        }
        
	}
	
}