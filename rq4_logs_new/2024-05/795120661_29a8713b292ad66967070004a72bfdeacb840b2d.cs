#nullable enable
using System;
using UnityEngine;

namespace Daipan.LevelDesign.Enemy.Scripts;

public sealed class EnemyDefeatParamsManager : ScriptableObject
{
   [SerializeField] EnemyQuickDefeatParam enemyQuickDefeatParam = null!;
   [SerializeField] EnemySlowDefeatParam enemySlowDefeatParam = null!;
}
[Serializable]
public sealed class EnemyQuickDefeatParam
{
}
[Serializable]
public sealed class EnemySlowDefeatParam
{
   [Header("この数の敵が特定の座標を超えるとアンチコメントが生成される")]
   [Min(0)]
   public int slowDefeatThreshold = 5;
   
}