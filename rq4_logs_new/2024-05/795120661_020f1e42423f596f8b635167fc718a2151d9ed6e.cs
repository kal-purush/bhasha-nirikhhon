using System;
using Daipan.Comment.Scripts;
using Daipan.LevelDesign.Comment.Scripts;
using UnityEngine;
using VContainer;

namespace Daipan.Comment.MonoScripts
{
    public  class AntiMono : MonoBehaviour
    {
        [SerializeField] SpriteRenderer spriteRenderer = null!;

        readonly float _despawneTime = 5f; // todo : パラメータから受け取るようにする
        float _timer;

        CommentEnum CommentEnum { get; set; } = CommentEnum.None;

        void Update()
        {
            _timer += Time.deltaTime;
            if (_timer > _despawneTime) {
                // todo : 仕様になかったのでいったんコメントアウト
                // Despawn();
            }
        }

        public event EventHandler<DespawnEventArgs>? OnDespawn;



        public void SetParameter(CommentEnum commentEnum)
        {
            CommentEnum = commentEnum;

            // コメントの背景は一旦なし
            // spriteRenderer.sprite = _commentParamsServer.GetSprite(commentEnum);
            // Debug.Log($"spriteRenderer.sprite : {spriteRenderer.sprite}");
        }

        public void Despawn()
        {
            var args = new DespawnEventArgs(CommentEnum);
            OnDespawn?.Invoke(this, args);
            Destroy(gameObject);
        }


    } 
}
