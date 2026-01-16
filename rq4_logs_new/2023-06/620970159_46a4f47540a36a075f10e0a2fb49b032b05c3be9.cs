// script for controlling an enemy
using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.AI;


public class EnemyController : MonoBehaviour
{
    [SerializeField] Enemy enemy;
    [SerializeField] GameObject player;
    [SerializeField] GameObject wolf;
    private NavMeshAgent enemyAgent;
    int target;

    List<Vector2>TargetPosition = new List<Vector2>();


    // Start is called before the first frame update
    void Start()
    {
        enemyAgent = GetComponent<NavMeshAgent>();
        enemyAgent.updateRotation = false;
        enemyAgent.updateUpAxis = false;
        InitializeTargetPositions();
    }


    // Update is called once per frame
    void Update()
    {

        //enemyAgent.SetDestination(TargetPosition[target]);
        
        if(enemy.name == "Impaler" || enemy.name == "Marauder")
        {
            enemyAgent.SetDestination(player.transform.position);
        } else if(enemy.name == "Grim")
        {
            enemyAgent.SetDestination(wolf.transform.position);
        }
        
    }

    private void InitializeTargetPositions()
    {
       
            Vector2 playerTarget = new Vector2(player.transform.position.x, player.transform.position.y);
            Vector2 wolfTarget = new Vector2(wolf.transform.position.x, wolf.transform.position.y);
            TargetPosition.Add(playerTarget);
            TargetPosition.Add(wolfTarget);
            target = UnityEngine.Random.Range(0, 2);
            Debug.Log("player target: "+ playerTarget);
            Debug.Log("wolf target: " + wolfTarget);

        
    }

}