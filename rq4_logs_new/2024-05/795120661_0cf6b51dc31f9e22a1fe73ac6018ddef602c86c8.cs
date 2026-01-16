#nullable enable
using System;
using UnityEngine;
using UnityEngine.Serialization;

namespace Daipan.LevelDesign.Player.Scripts
{

    [CreateAssetMenu(fileName = "Data", menuName = "ScriptableObjects/Player/PlayerParameters", order = 1)]
    public sealed class PlayerParameter : ScriptableObject
    {
        [Header("プレイヤーのレベルデザインはこちら！！")]
        [Space(30)]

        [Header("プレイヤーのHP")]
        [Min(0)]
        public int HPAmount;

        [Header(("プレイヤーの攻撃力"))]
        [Min(0)]
        public int attackAmount;
    }
}