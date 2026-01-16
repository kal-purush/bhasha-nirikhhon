using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BerserkPhase : BasePhase
{
    private MonsterMeleeAttack _meleeAttack;
    private BerserkAttack _berserkAttack;
    private BossController _bossController;
    private Animator _animator;

    
    public float meleeDamage = 15;
    public float berserkDamage = 30;
    public float startAttackTimeout = 5;
    public float endAttackTimeout = 10;

    
    void Start()
    {
        this.enabled = false;
    }
    
    public override void StartPhase()
    {
        _animator = GetComponent<Animator>();

        _bossController = GetComponent<BossController>();
        _bossController.minDistance = 1;
        
        _animator.SetTrigger("BackToIdle");

        AddAttack();
    }
    
    public override void AddAttack()
    {
        _meleeAttack = gameObject.AddComponent<MonsterMeleeAttack>();
        _meleeAttack.damage = meleeDamage;
        _berserkAttack = gameObject.AddComponent<BerserkAttack>();
        _berserkAttack.damage = berserkDamage;
        _berserkAttack.startAttackTimeout = startAttackTimeout;
        _berserkAttack.endAttackTimeout = endAttackTimeout;
    }
    
    public override void RemoveAttack()
    {
        Destroy(_meleeAttack);
        Destroy(_berserkAttack);
    }
    
    public override void EndPhase()
    {
        _animator.SetTrigger("BackToIdle");
        RemoveAttack();
    }
}