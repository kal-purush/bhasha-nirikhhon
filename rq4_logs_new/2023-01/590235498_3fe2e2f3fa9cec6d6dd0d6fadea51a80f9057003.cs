using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BigClawChargedAttackState : BossAttackState
{
    private float channelTime = 3.0f;
    private bool attackPerformed = false;

    private BossStateMachine stateMachine;
    public override void OnStateEnter(BossStateMachine _stateMachine)
    {
        if (!stateMachine) stateMachine = _stateMachine;
        _stateMachine.agent.SetDestination(_stateMachine.transform.position);
        _stateMachine.animator.SetTrigger("chargedStart");
        _stateMachine.agent.isStopped = true;

        BigClawFeet.OnCapture += OnBallReturned;
        attackPerformed = false;
        channelTime = 3.0f;

        base.OnStateEnter(_stateMachine);
        
    }

    public override void OnStateExit(BossStateMachine _stateMachine)
    {
        _stateMachine.agent.isStopped = false;
        BigClawFeet.OnCapture -= OnBallReturned;
    }

    public override void OnStateUpdate(BossStateMachine _stateMachine)
    {
        if(channelTime > 0.0f)
        {
            channelTime -= Time.deltaTime;
            _stateMachine.bossBase.LookAtPlayer();
        } else
        {
            if (attackPerformed) return;
            _stateMachine.animator.SetTrigger("chargedShoot");
            _stateMachine.bossBase.PerformSpecialAttack();
            attackPerformed = true;
        }


    }
    //private void OnEnable()
    //{
        
    //}

    //private void OnDisable()
    //{
     

    //}

    private void OnBallReturned()
    {
        Debug.Log("My ball returned");
        stateMachine.animator.SetTrigger("chargedEnd");
        stateMachine.ChangeState(stateMachine.idleState);
    }
}