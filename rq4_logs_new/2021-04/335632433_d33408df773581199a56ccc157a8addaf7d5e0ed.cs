using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class FlyE : MonoBehaviour
{
    public StickE stickE;
    bool flyHigh;
    float seconds;
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void FixedUpdate()
    {
        flyHigh = stickE.flyOn;
        if (flyHigh == true)
        {
            seconds += Time.deltaTime;
            if(seconds>=0.5)
            {
                transform.Translate(0, 0.3f, 0);
                Debug.Log("aa");
            }
            
        }
    }
}