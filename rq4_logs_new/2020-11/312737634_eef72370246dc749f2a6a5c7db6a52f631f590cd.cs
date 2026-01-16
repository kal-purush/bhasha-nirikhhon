using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Spawner : MonoBehaviour
{
    private Vector3 spawnPosition;
    private Transform botLow;
    private Transform botHigh;
    private Transform topLow;
    private Transform topHigh;
    private GameObject gate;
    private Transform pivot;

    void Awake()
    {
        botLow=GameObject.FindWithTag("spawnerBotLow").GetComponent<Transform>();
        botHigh=GameObject.FindWithTag("spawnerBotHigh").GetComponent<Transform>();
        topLow=GameObject.FindWithTag("spawnerTopLow").GetComponent<Transform>();
        topHigh=GameObject.FindWithTag("spawnerTopHigh").GetComponent<Transform>();

        gate=Resources.Load("BoomBarrier") as GameObject;
        pivot=GameObject.FindWithTag("pivot").GetComponent<Transform>();;
    }

    void Update(){
        if(Input.GetKey(KeyCode.M) && Input.GetKeyDown(KeyCode.J)){//Cheat code for spawning gates!
            SpawnGate();
        }
    }
    
    void SpawnGate()
    {
        float interpolant=Random.Range(0.0f,1.0f);
        int type=Random.Range(0,2);//0=low, 1=high
        Quaternion rotation=botLow.rotation;
        if(type==0){
            spawnPosition = Vector3.Lerp(botLow.position,botHigh.position,interpolant);            
        }
        else{
            spawnPosition = Vector3.Lerp(topLow.position,topHigh.position,interpolant);
        }
        Instantiate(gate,spawnPosition,rotation,pivot);
    }
}