using UnityEngine;
using System.Collections;

public class CarMove : MonoBehaviour {
    double speed = 0.25;
    bool go = true;

	// Use this for initialization
	void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
        if (go)
            this.gameObject.transform.Translate(new Vector3(0, 0, (float)speed));
	}

    void OnTriggerEnter(Collider other)
    {
        if (other.gameObject.tag == "Stop")
            go = false;
    }

    void OnTriggerExit(Collider other)
    {
        if (other.gameObject.tag == "Stop")
            go = false;
    }
}