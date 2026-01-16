using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class FuelCapacity : MonoBehaviour
{
    public float fuel;
    public float reductionRatio;
    // Start is called before the first frame update
    void Start()
    {
        fuel = 100f;
        reductionRatio = 2f;
    }

    // Update is called once per frame
    void Update()
    {
        fuel-= (Time.deltaTime * reductionRatio) % 100;
        if(fuel == 0){
            terminate();
        }
    }

    void terminate(){
        //to traverse to next scene;
        Application.Quit();
    }

    void ReductionCloud(float cloudReduction) {
        fuel = (fuel - cloudReduction) % 100;
    }

    void FuelCollection(float increment) {
        fuel = (fuel + increment) % 100;
    }
}