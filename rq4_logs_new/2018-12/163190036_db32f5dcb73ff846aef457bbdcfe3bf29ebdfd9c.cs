using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.AI;

public class OwlSentinelScript : MonoBehaviour {
	[SerializeField] float owlLifeTime;
	TrailRenderer trailRenderer;
	NavMeshAgent navMeshAgent;
	// Use this for initialization
	void Start () {
		trailRenderer = GetComponent<TrailRenderer>();
		navMeshAgent = GetComponent<NavMeshAgent>();
		navMeshAgent.SetDestination(GameObject.FindGameObjectWithTag("Player").GetComponent<PlayerScript>().spawnPoint);
	}
	
	// Update is called once per frame
	void Update () {
		if (Input.GetKeyDown(KeyCode.LeftShift))
		{
			Destroy(gameObject);
		}
		
	}
    
}