using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MP_Acceleration : MP_Controls {

    [SerializeField] float speed;
    [SerializeField] float maxSpeed;
    bool stopped;
    GameObject floor;
    float minSpeed = 0;

	// Use this for initialization
	void Start () {
        floor = GameObject.Find("floor");
	}
	
	// Update is called once per frame
	void Update () {
		
	}

    public void Accelerate()
    {
        print("Accelerate");
        stopped = false;
        floor.transform.Translate(Vector2.left * (speed += (Time.deltaTime * .5f)));
        if(speed > maxSpeed)
        {
            speed = maxSpeed;
        }
        
    }

    public void Decelerate()
    {
        if (!stopped)
        {
            floor.transform.Translate(Vector2.left * (speed -= (Time.deltaTime * .5f)));
            if (speed < minSpeed)
            {
                speed = minSpeed;
                stopped = true;
            }
            
        }
    }

}