using System;
using System.Collections;
using System.Collections.Generic;
using UnityEditor.Overlays;
using UnityEngine;

public class HauntingBlade : EnemyClass
{
    [SerializeField] private Collider2D slashCollider;
    [SerializeField] private Cooldown slashAttackDuration;
    [SerializeField] private Cooldown slashAttackCooldown;

    private bool neutralSlash;

    public bool NeutralSlash { get { return neutralSlash; } }

    protected override void EnemyMoveset()
    {
        if (canAttack == true && slashAttackDuration.CurrentProgress is Cooldown.Progress.Ready && slashAttackCooldown.CurrentProgress is Cooldown.Progress.Ready)
        {
            neutralSlash = true;
            slashCollider.enabled = true;
            slashAttackDuration.StartCooldown();
            Debug.Log("Slashing");
        }

        if (slashAttackDuration.CurrentProgress is Cooldown.Progress.Finished)
        {
            neutralSlash = false;
            slashCollider.enabled = false;
            slashAttackCooldown.StartCooldown();
            slashAttackDuration.ResetCooldown();
        }

        if (slashAttackCooldown.CurrentProgress is Cooldown.Progress.Finished)
        {
            slashAttackCooldown.ResetCooldown();
        }
    }
}