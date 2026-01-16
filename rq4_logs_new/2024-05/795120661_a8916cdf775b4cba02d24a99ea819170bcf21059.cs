using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;


namespace Daipan.LevelDesign.Tower.Scripts
{
    [CreateAssetMenu(fileName = "Data", menuName = "ScriptableObjects/Tower/Parameters", order = 1)]
    public sealed class TowerParams : ScriptableObject
    {
        [Header("タワーのレベルデザインはこちら！！")]
        [Space(30)]

        [Header("HPごとのタワーのスプライト変更")]
        public List<TowerSprite> towerSprites;
    }

    [Serializable]
    public sealed class TowerSprite
    {
        [Header("HPがnを切ったらSpriteを変更")]
        public float hpThreshold = 0;
        public Sprite sprite;
    }
}