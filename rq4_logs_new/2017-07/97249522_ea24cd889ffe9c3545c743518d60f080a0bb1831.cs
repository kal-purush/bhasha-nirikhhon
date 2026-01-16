using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Laser : MonoBehaviour {

	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
        int face = PlayerControl.face;
        switch (face)
        {
            case 0:
                gameObject.transform.position += new Vector3(0, 0.4f, 0);
                break;
            case 1:
                gameObject.transform.position += new Vector3(0, -0.4f, 0);
                break;
            case 2:
                gameObject.transform.position += new Vector3(-0.4f, 0, 0);
                break;
            case 3:
                gameObject.transform.position += new Vector3(0.4f, 0, 0);
                break;
        }
    }
}