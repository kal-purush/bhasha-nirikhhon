using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Watering : MonoBehaviour {
    public Vector3 objectRotation, instantiateWaterLocation;
    public GameObject water, instantiateWaterObject;        
    public float interval, timer, rotateTriggerUp, rotateTriggerDown;
	// Use this for initialization
	void Start () {
        resetTimer();
	}
	
	// Update is called once per frame
	void Update () {
        objectRotation = transform.eulerAngles;
        instantiateWaterLocation = instantiateWaterObject.transform.position;
        //objectRotation.z *= 100;
        //Check the rotation of the watering can to see if we need to instantiate water
        //315, 275
        if(objectRotation.z < rotateTriggerUp && objectRotation.z > rotateTriggerDown)
        {
            if(getElapsedTime()> interval)
            {
                Debug.Log("Instantiate the water babbbby");
                Instantiate(water, instantiateWaterLocation + new Vector3(0.1f,Random.Range(-0.4f,-0.1f), Random.Range(-0.1f, 0.1f)), Quaternion.identity);
                resetTimer();
            }
            
        }
	}

    public void resetTimer()
    {
        timer = Time.time;
    }
    public float getElapsedTime()
    {
        return Time.time - timer;
    }
}