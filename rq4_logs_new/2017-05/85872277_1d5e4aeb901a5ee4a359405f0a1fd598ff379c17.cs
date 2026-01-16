using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class AddBoxColliderToPlayer : MonoBehaviour {
    private GameObject player;
    public float Rangex, Rangey, Rangez;
    BoxCollider box;
	// Use this for initialization
	void Start () {
        player = GameObject.Find("SpineMid");
    }
	
	// Update is called once per frame
	void Update () {
        player = GameObject.Find("SpineMid");
        if ((player != null))
        {
            player.GetComponent<SphereCollider>().enabled = false;
            box = player.GetComponent<BoxCollider>();
            box.size = new Vector3(Rangex, Rangey,  Rangez);
        }
        

    }
}