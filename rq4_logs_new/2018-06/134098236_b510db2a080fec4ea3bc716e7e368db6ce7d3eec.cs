using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GameManager : MonoBehaviour {

    public bool holding;
    public GameObject item;
    public GameObject player;

	// Use this for initialization
	void Start () {
		
	}
	
	// Update is called once per frame
	void Update () {
        if (item)
        {
            player.GetComponent<ThrowingArcRender>().enabled = true;
        }
        else
        {
            player.GetComponent<ThrowingArcRender>().enabled = true;
        }
		
	}
}