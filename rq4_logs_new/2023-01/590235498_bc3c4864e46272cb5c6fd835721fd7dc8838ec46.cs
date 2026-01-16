using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BigClawAttackState : BossAttackState
{
    private float timer;
    public override void OnStateEnter(BossStateMachine _stateMachine)
    {
        timer = 1.0f;
        base.OnStateEnter(_stateMachine);
     }

    public override void OnStateExit(BossStateMachine _stateMachine)
    {
       // _stateMachine.agent.isStopped = false;
    }

    public override void OnStateUpdate(BossStateMachine _stateMachine)
    {
        Debug.Log("Attacking");
        if (timer > 0.0f) timer -= Time.deltaTime;
        else _stateMachine.ChangeState(_stateMachine.idleState);
    }
}