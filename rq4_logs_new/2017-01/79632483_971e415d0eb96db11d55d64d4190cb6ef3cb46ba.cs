using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ShotControl : MonoBehaviour {

	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
		transform.position += transform.forward * Time.deltaTime * 100f;
    }

    void OnTriggerEnter(Collider other)
    {
        Debug.Log("Laser hit " + other.gameObject.tag);
        if(other.gameObject.tag == "Enemy")
        {
            other.gameObject.SendMessage("TakeDamage", 10);
        }
    }
}