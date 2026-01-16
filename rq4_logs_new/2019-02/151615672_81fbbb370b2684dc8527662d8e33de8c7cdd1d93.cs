using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AbsorbFollowScript : MonoBehaviour {
    private Transform playerLocation;
    // Use this for initialization
    void Start ()
    {
        playerLocation = GameObject.Find("Player").transform;
    }
	
	// Update is called once per frame
	void Update ()
    {
        playerLocation = GameObject.Find("Player").transform;
        gameObject.transform.position = playerLocation.transform.position;
        gameObject.transform.rotation = playerLocation.transform.rotation;
    }
}