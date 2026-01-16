using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.AI;

public class Chase : State
{
    public GameObject glowObj;
    public GameObject lightDecider;
    public DecideLight light;

    public Chase(GameObject _npc, NavMeshAgent _agent, Animator _anim, Transform _player)
        : base(_npc, _agent, _anim, _player)
    {
        name = STATE.CHASE;
        agent.speed = 2; //speed is only important if agent has a pathg
        agent.isStopped = false;
    }

    public override void Enter()
    {
        lightDecider = GameObject.FindGameObjectWithTag("lightTracker");
        light = lightDecider.GetComponent<DecideLight>();
        Debug.Log("Chase");

        base.Enter();
    }
    public override void Update()
    {
        if ((agent.transform.position - player.transform.position).magnitude < 4)
        {
            agent.SetDestination(player.transform.position);
            if ((agent.transform.position - player.transform.position).magnitude < 2)
            {
                nextState = new Attack(npc, agent, anim, player);
                stage = EVENT.EXIT;
            }

        }

        else if (light.target != null)
        {
            agent.SetDestination(light.target.transform.position);

            if ((agent.transform.position - light.target.transform.position).magnitude < 1)
            {
                light.target.GetComponent<Light>().enabled = false;
                nextState = new Idle(npc, agent, anim, player);
                stage = EVENT.EXIT;

            }
        }

        else
        {
            nextState = new Idle(npc, agent, anim, player);
            stage = EVENT.EXIT;
        }

    }
    public override void Exit()
    {
        // anim.ResetTrigger("isWalking"); //takes out any sitting animations that didnt run
        base.Exit();
    }
}