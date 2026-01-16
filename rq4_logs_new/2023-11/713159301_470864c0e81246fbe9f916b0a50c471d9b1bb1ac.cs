using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Lizard : Enemy
{
    // Start is called before the first frame update
    void Start()
    {
        states.Add(new Idle());//0
        states.Add(new Pacing());
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}