using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.AI;
using MoveState = EnemyState.MoveState;
using Enchants = EnemyState.Enchants;
using System.Security.Cryptography;
using UnityEditor.PackageManager;
using System;
using System.Runtime.ExceptionServices;

[RequireComponent(typeof(NavMeshAgent))]
[RequireComponent(typeof(EnemyState))]
public class EnemyBrainDefault : MonoBehaviour, IEnemyBrainBase
{
    public void AddEnchant(Enchants enchant, float time)
    {
        throw new NotImplementedException();
    }

    public void Blind(float time)
    {
        throw new NotImplementedException();
    }

    public void Default()
    {
        throw new NotImplementedException();
    }

    public bool IsAttackable()
    {
        throw new NotImplementedException();
    }

    public void Stan(float time)
    {
        throw new NotImplementedException();
    }

    public void Think()
    {
        throw new NotImplementedException();
    }
}