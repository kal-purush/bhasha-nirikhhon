#nullable enable
using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;


namespace Enemy
{
    [Serializable]
    public sealed class EnemyAttackParameter
    {
        public int AttackAmount;
    }

    [Serializable]
    public sealed class EnemyHPParameter
    {
        public int HPAmount;
    }

    [Serializable]
    public sealed class EnemyMovementParameter
    {
        public float MovementAmount;
    }


    [CreateAssetMenu(fileName = "Data", menuName = "ScriptableObjects/EnemyParameter", order = 1)]
    public sealed class EnemyParameter : ScriptableObject
    {
        public EnemyAttackParameter attackParameter = null;
        public EnemyHPParameter hpParameter = null;
        public EnemyMovementParameter movementParameter = null;
    }
}