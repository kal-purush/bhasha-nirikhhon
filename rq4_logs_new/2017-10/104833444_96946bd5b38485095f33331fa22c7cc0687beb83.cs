using System.Collections;
using System.Collections.Generic;
using UnityEngine;

/*
Name: Rabah Habiss
ID: 2268381
Email: habis102@mail.chapman.edu
Course: CPSC-340-01
Assignment: Semester Project

Description: Collectible Script. Attach to star shard objects
*/

public class StarShards : MonoBehaviour
{

    private bool isCollected = false;
    private Renderer render;
    private LevelManager level;

    // Use this for initialization
    void Start()
    {
        level = GameObject.Find("LevelManager").GetComponent<LevelManager>();
        render = GetComponent<Renderer>();
    }

    void OnTriggerEnter(Collider other)
    {
        if (other.tag == "Player" && !isCollected)
        {
            isCollected = true;
            render.enabled = false;
            level.CollectShard();
        }
    }

}