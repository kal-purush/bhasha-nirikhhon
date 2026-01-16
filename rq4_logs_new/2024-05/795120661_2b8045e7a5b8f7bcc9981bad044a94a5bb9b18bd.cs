#nullable enable
using System.Collections.Generic;
using Daipan.LevelDesign.Comment.Scripts;
using UnityEngine;

namespace Daipan.Comment.Scripts
{
    [CreateAssetMenu(fileName = "Data", menuName = "ScriptableObjects/Comment/ManagerParameters", order = 1)]
    public class CommentManagerParams : ScriptableObject
    {
        [Header("コメント全体のレベルデザインはこちら！！")]
        [Space(30)]


        [Header("これをオンにするとコメントの流れる速度が一定になります。")]
        public bool isAdaptSameSpeed;
        [Min(0)] public float commentSpeed;


        [Space(30)]
        [Header("いらいら度による各コメントの吹っ飛び率は未実装です")]
        [Space(30)]



        [Header("使用するコメントを設定してください。")]
        public List<CommentParams> commentParams;
    }
}