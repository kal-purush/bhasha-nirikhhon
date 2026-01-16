using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CameraControl : MonoBehaviour {
    public GameObject target;
    public GameObject background;
    private float time;
	// Use this for initialization
	void Start () {
        time = 0;
        //this.transform.parent = target.transform;
	}
	
	// Update is called once per frame
	void Update () {
        time += Time.deltaTime;
        if (time <= 2)
        {
            background.transform.localScale += new Vector3(Time.deltaTime * 0.5f, Time.deltaTime * 0.5f, 0);
            this.transform.position = Vector3.MoveTowards(this.transform.position, target.transform.position, 0.02f);
        }
        //this.transform.position = target.transform.position;
	}
}