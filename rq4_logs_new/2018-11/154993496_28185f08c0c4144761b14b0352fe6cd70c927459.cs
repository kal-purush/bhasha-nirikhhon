using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.AI;
/*
 * Author Brennan J.C Kersey
 * Script Purpose: To control movement of AI
 */
public class BasicAI : MonoBehaviour
{
    private NavMeshAgent Navagent; // NavMesh agent for Unity tracking
    private GameObject target; // target for the enemy to move towards
    // Use this for initialization
    void Start()
    {
        Navagent = GetComponent<NavMeshAgent>();
        target = GameObject.FindGameObjectWithTag("Player");
	}
	
	// Update is called once per frame
	void Update ()
    {
        if(target==null) // conditional to stop enemies form moving after playerdeath
        {
            target = this.gameObject;
        }
        Navagent.SetDestination(target.transform.position);
	}
}