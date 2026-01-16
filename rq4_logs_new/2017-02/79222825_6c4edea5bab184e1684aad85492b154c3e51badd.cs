using UnityEngine;
using System.Collections;

public class IceCheck : MonoBehaviour {

    MovementScript player;

	// Use this for initialization
	void Start ()
    {
        player = GameObject.Find("Player").GetComponent<MovementScript>();
	}
	
	// Update is called once per frame
	void Update ()
    {
	    
	}

    void OnTriggerStay2D(Collider2D other)
    {
        if (other.gameObject.tag != "Ice")
            return;

        player.on_ice = true;
    }

    void OnTriggerExit2D(Collider2D other)
    {
        if (other.gameObject.tag != "Ice")
            return;

        player.on_ice = false;
    }
}