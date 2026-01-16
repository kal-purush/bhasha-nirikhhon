using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BeeAttackState : BeeBaseState
{
    public override void EnterState(BeeStateManager beeSManager)
    {
        Debug.Log("start attack state of bee");
        beeSManager?.StartShooting();
    }
    public override void UpdateState(BeeStateManager beeSManager, Transform target, Transform player)
    {
        beeSManager?.RotateTowards(player);
    }
    public override void OnCollision(BeeStateManager beeSManager, Collider2D collision)
    {
        beeSManager?.CollisionWithPlayer(collision);
    }

    public override void OnCollide(BeeStateManager beeSManager, Collision2D collision)
    {

    }
}