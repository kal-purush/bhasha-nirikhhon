using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MovingPhysics : MonoBehaviour {

	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
		
	}

    void OnTriggerStay(Collider other)
    {

        if (other.gameObject.tag == "Player")
        {
            other.transform.parent = this.transform;

        }
    }

    void OnTriggerExit(Collider other)
    {
        if (other.gameObject.tag == "Player")
        {
            other.transform.parent = null;

        }
    }
}