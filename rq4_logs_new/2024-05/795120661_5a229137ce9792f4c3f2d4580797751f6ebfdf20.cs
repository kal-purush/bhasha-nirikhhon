#nullable enable
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using VContainer;


namespace Daipan.LevelDesign.Comment.Scripts
{
    public class CommentParamsServer
    {
        readonly CommentParams _commentParams = null!;
        readonly CommentPosition _commentPosition = null!;

        [Inject]
        CommentParamsServer (CommentParams commentParams, CommentPosition commentPosition)
        {
            _commentParams = commentParams;
            _commentPosition = commentPosition;
        }

        public Vector3 GetSpawnedPosiion()
        {
            return _commentPosition.CommentSpawnedPoint.position;
        }

        public Vector3 GetDespawnedPosition()
        {
            return _commentPosition.CommentDespawnedPoint.position;
        }

        public Transform GetCommentParent()
        {
            return _commentPosition.CommentParent;
        }

    }
}