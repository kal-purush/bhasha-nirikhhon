using System;
using Daipan.Comment.Scripts;
using Daipan.LevelDesign.Comment.Scripts;
using UnityEngine;
using VContainer;

namespace Daipan.Comment.MonoScripts;

public sealed class AntiCommentMono : MonoBehaviour
{
    [SerializeField] SpriteRenderer spriteRenderer = null!;
    CommentParamsServer _commentParamsServer = null!;

    CommentEnum CommentEnum { get; set; } = CommentEnum.None;

    void Update()
    {
        // var direction = (_commentParamsServer.GetDespawnedPosition() - transform.position).normalized;
        // transform.position += direction * _commentParamsServer.GetSpeed(_commentEnum) * Time.deltaTime;
        // if ((transform.position - _commentParamsServer.GetDespawnedPosition()).magnitude < float.Epsilon) _commentCluster.Remove(this);
    }

    public event EventHandler<DespawnEventArgs>? OnDespawn;


    [Inject]
    public void Initialize(
        CommentParamsServer commentParamsServer,
        CommentCluster commentCluster
    )
    {
        _commentParamsServer = commentParamsServer;
    }

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

    public void BlownAway()
    {
    }
}