using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.AI;

public class Goto : Instruction
{
    public Vector3 location;
    //private float callForOrder;
    //private int roomNb = 0;
    private float timer = 1;
    NavMeshAgent entityAgent;

    #region Methods

    public Goto(Vector3 location, float timer, Entity entity)
    {
        this.location = location;
        this.timer = timer;
        instructionRunner = entity;
        entityAgent = instructionRunner.GetComponent<NavMeshAgent>();
    }

    override public void Execute()
    { 
        if (instructionRunner.transform.position.x != location.x && instructionRunner.transform.position.z != location.z)
        {
            entityAgent.SetDestination(location);
        }
        else
        {
            if (timer > 0)
            {
                timer -= Time.deltaTime;
            }
            else
            {
                instructionRunner.instructionEvent.Invoke(null);
            }
        }
    }
    #endregion
}