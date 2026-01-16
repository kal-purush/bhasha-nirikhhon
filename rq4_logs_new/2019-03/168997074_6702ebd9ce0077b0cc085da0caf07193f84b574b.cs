using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.AI;

public class Investigate : Order
{
    NavMeshAgent agent;
    //private int roomNb = 0;
    public float searchTimer;
    public float navigationTimer;

    void Start()
    {

    }

    private void Awake()
    {
    }

    override public void ExtractInstructions(Entity entity)
    {
        instructions = new List<Instruction>();
        foreach (GameObject obj in routine)
        {
            instructions.Add(new SearchRoom(obj.GetComponentInChildren<Room>(), searchTimer, navigationTimer, entity));
        }
    }

    // Update is called once per frame
    void Update()
    {

    }
}