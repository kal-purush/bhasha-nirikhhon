using System.Collections;
using System.Collections.Generic;
using System;
using UnityEngine;

public class AppearItem : MonoBehaviour
{
    // Start is called before the first frame update
    public GameObject master;
    public bool ItemAppear = false;
    private double Probability = (0.05+0.01*(Data.ProbabilityLevel-1))/100;
    public GameObject Item;
    void Start()
    {
        Item.SetActive(false);
    }

    // Update is called once per frame
    void Update()
    {
        if(!master.GetComponent<MyTimer>().isMarginTimeUsing){
            double R = UnityEngine.Random.value;
            Debug.Log(R);
            if(R<=Probability){
                Item.SetActive(true);
            }
        }
    }
}