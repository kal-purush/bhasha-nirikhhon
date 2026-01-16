#nullable enable
using System.Collections;
using System.Collections.Generic;
using UnityEngine;


namespace Daipan.LevelDesign.Comment.Scripts
{
    public class CommentPosition : MonoBehaviour
    {
        [Header("コメントの座標たちをセットする場所です！")]
        [Space(30)]
        
        
        
        [Header("コメントの生成位置")]
        [Tooltip("コメントの生成位置を示すGameObjectを入れて！！")]
        [SerializeField]
        GameObject CommentSpawnedPoint = null!;

        [Header("コメントの消滅位置")]
        [Tooltip("コメントの消滅位置を示すGameObjectを入れて！！")]
        [SerializeField]
        GameObject CommentDespawnedPoint = null!;
    }
}