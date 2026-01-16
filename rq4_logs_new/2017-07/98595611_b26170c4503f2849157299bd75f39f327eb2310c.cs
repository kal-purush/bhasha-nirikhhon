using System.Collections;
using System.Collections.Generic;
using DigitalRuby.LightningBolt;
using UnityEngine;

public class Checkpoint : MonoBehaviour {

    public bool active = false;
    private RespawnManager respawnManager;
    public LineRenderer lr;
    public LightningBoltScript lb;
    public BatteryRotate br;

    private void Start()
    {
        respawnManager = GameObject.FindGameObjectWithTag("Player").GetComponent<RespawnManager>();
    }

    private void Update()
    {
        if (active)
        {
            lr.enabled = true;
            lb.enabled = true;
            br.enabled = true;
        }
        else
        {
            lr.enabled = false;
            lb.enabled = false;
            br.enabled = false;
        }
    }

    private void OnCollisionEnter(Collision collision)
    {
        if(collision.gameObject.CompareTag("Player Projectile"))
        {
            respawnManager.SetActiveCheckpoint(this);
            active = true;
        }
    }

}