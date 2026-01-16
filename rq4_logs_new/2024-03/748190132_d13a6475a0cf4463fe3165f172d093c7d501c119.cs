using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.AI;

public class ChasePoints : MonoBehaviour
{

    public NavMeshAgent agent;

    // I NEED TO CHECK IF THE CHASE POINT IS ON THE NAV MESH IF IT IS NOT
    //I WILL NEED TO RETURN A FALSE TO THE GAME MANAGER SO THE ENEMY CAN CHOOSE TO ATTACK THE PLAYER INSTEAD OF THE CHASE POINT?
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        Debug.Log(agent.isOnNavMesh);
    }
}