using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EngineFlame : MonoBehaviour
{
    public PlayerShipController parentShip;
    public float minLifetime = 0;
    public float maxLifetime = 2;
    public bool reverse; //true for forward facing engines that fire when reversing
    private ParticleSystem.MainModule _particleSystemMain;

    // Start is called before the first frame update
    void Start()
    {
        if (parentShip == null)
            Debug.LogError("No Parent Ship On EngineFlame");

        _particleSystemMain = this.GetComponent<ParticleSystem>().main;
    }

    // Update is called once per frame
    void Update()
    {
        var targetDirection = parentShip.accelerating;
        if (reverse)
            targetDirection = parentShip.reversing;
        
        if (targetDirection)
            _particleSystemMain.startLifetime = Mathf.Lerp(maxLifetime, minLifetime, Time.deltaTime); 
        else 
            _particleSystemMain.startLifetime = Mathf.Lerp(minLifetime, maxLifetime, Time.deltaTime); 
    }

}