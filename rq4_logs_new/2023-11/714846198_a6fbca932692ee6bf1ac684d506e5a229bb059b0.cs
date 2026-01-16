using UnityEngine;
using System.Collections.Generic;

[CreateAssetMenu(fileName = "Data", menuName = "ScriptableObjects/CreateEnemyParamAssets", order = 1)]

public class EnemyParamAsset : ScriptableObject
{
    [SerializeField] int _unitID;
    [SerializeField] int _unitHP;
    [SerializeField] float _unitAtkSpan;　//攻撃ごとの間隔
    [SerializeField] float _unitAtkPeriodicTime; //１回の攻撃にかかる時間
    [SerializeField] TeamB_TD.Enemy.MoveCategorize moveCategorize;
    //public List<EnemyParam> EnemyParamList = new List<EnemyParam>();

}

[System.Serializable]
public class EnemyParam
{
    [SerializeField] int _unitID;
    [SerializeField] int _unitHP;
    [SerializeField] float _unitAtkSpan;　//攻撃ごとの間隔
    [SerializeField] float _unitAtkPeriodicTime; //１回の攻撃にかかる時間
    [SerializeField] TeamB_TD.Enemy.MoveCategorize moveCategorize;

}