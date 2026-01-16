#nullable enable
using System.Collections;
using System.Collections.Generic;
using UnityEngine;


namespace Daipan.LevelDesign.Enemy.Scripts
{
    public class EnemyPosition : MonoBehaviour
    {
        [Header("エネミーの座標たちをセットする場所です！")]
        [Space(30)]


        [Header("エネミーの生成位置")]
        [Tooltip("エネミーの生成位置を示すGameObjectを入れて！！複数可")]
        public List<Transform> EnemySpawnedPoints;


        [Header("エネミーの消滅位置")]
        [Tooltip("エネミーの消滅位置を示すGameObjectを入れて！！")]
        public Transform EnemyDespawnedPoint;

    }
}