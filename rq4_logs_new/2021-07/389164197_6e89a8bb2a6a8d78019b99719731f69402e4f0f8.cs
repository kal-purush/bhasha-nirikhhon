using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Obstacle5_Behavior : MonoBehaviour
{
    private float ticks = 0.0f;
    private const float INTERVAL = 5f;
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        ticks += Time.deltaTime;
        if(ticks >= INTERVAL)
        {
            this.GetComponent<Rigidbody>().AddForce(1000f, 0f, 0f);
            ticks = 0f;
        }
    }
}