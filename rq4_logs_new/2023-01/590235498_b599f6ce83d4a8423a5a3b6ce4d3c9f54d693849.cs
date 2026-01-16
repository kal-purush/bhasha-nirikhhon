using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BigClawMeleeAttackState : BossAttackState
{
    private float timer;
    public override void OnStateEnter(BossStateMachine _stateMachine)
    {
        _stateMachine.animator.SetTrigger("meleeAtk");
        _stateMachine.agent.isStopped = true;
        timer = 2.0f;
        base.OnStateEnter(_stateMachine);
    }

    public override void OnStateExit(BossStateMachine _stateMachine)
    {
        _stateMachine.agent.isStopped = false;
        _stateMachine.animator.SetBool("isMoving", false);
    }

    public override void OnStateUpdate(BossStateMachine _stateMachine)
    {

    }
}