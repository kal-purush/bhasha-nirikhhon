using System.Collections;
using System.Collections.Generic;
using UnityEngine;

/// <summary>
/// 設定されたArtsからランダムピックする
/// </summary>
public class EnemyArtsPickUp : MonoBehaviour
{
    /// <summary>
    /// アーツIdの取得
    /// </summary>
    public string GetArtsId { get; private set; }

    /// <summary>
    /// メインエフェクトの取得
    /// </summary>
    public NameDefinition.EffectName GetMainEffect { get; private set; }

    /// <summary>
    /// サブエフェクトの取得
    /// </summary>
    public NameDefinition.EffectName[] GetSubEffect { get; private set; }

    [SerializeField] NameDefinition.EffectName mainType;
    [SerializeField] List<string> artsTable = new List<string>()
    {
        "045"
    };

    private void Awake()
    {
        List<NameDefinition.EffectName> effects = new List<NameDefinition.EffectName>();
        var artsId = GetArtsIdPick();

        //サブエフェクトをセットする
        foreach(var effect in artsId)
        {
            var effectNum = (NameDefinition.EffectName)effect;
            if (GetMainEffect!= effectNum)
            {
                effects.Add(effectNum);
            }
        }

        GetSubEffect = effects.ToArray();
        GetArtsId = artsId;
    }

    //セットされているアーツからランダムピック
    string GetArtsIdPick()
    {
        var ranNum = Random.Range(0, artsTable.Count);
        return artsTable[ranNum];
    }
}