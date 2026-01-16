using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerAnimator : MonoBehaviour
{
    [SerializeField] private float moveDurationPerTile = 0.5f;
    [SerializeField] private float hopHeight = 1.0f;

    private GameObject playerPiece;
    private bool isAnimating = false; // Renamed from isMoving for clarity

    public bool IsAnimating => isAnimating; // Public getter

    // Method for Board to provide the piece reference
    public void SetPlayerPiece(GameObject piece)
    {
        this.playerPiece = piece;
    }

    public void MovePlayerPieceInstantly(BoardTile boardTile)
    {
        if (playerPiece == null || boardTile == null || boardTile.playerPlacement == null)
        {
            Debug.LogError($"Cannot instantly move player piece. playerPiece: {playerPiece}, boardTile: {boardTile}, playerPlacement: {boardTile?.playerPlacement}");
            return;
        }
        playerPiece.transform.position = boardTile.playerPlacement.position;
    }

    // Start the animation and provide a callback for when it's done
    public void AnimateMove(List<BoardTile> path, Action onComplete)
    {
        if (isAnimating)
        {
            Debug.LogWarning("Already animating.");
            return;
        }
        if (playerPiece == null)
        {
            Debug.LogError("Player piece reference is null in PlayerAnimator.");
            return;
        }
        if (path == null || path.Count <= 1)
        {
            Debug.LogWarning("Invalid path for animation.");
            onComplete?.Invoke(); // Call completion immediately if no move needed
            return;
        }

        StartCoroutine(AnimatePlayerMovementCoroutine(path, onComplete));
    }

    private IEnumerator AnimatePlayerMovementCoroutine(List<BoardTile> path, Action onComplete)
    {
        isAnimating = true;
        Debug.Log($"[Animator] Starting animation across {path.Count - 1} steps.");

        for (int i = 0; i < path.Count - 1; i++)
        {
            BoardTile segmentStartTile = path[i];
            BoardTile targetTile = path[i + 1];

            if (segmentStartTile.playerPlacement == null || targetTile.playerPlacement == null)
            {
                Debug.LogError($"[Animator] Missing playerPlacement transform on {segmentStartTile.name} or {targetTile.name}. Aborting move.");
                isAnimating = false;
                onComplete?.Invoke(); // Signal completion (even on error)
                yield break;
            }

            Vector3 startPos = (i == 0) ? playerPiece.transform.position : segmentStartTile.playerPlacement.position;
            Vector3 endPos = targetTile.playerPlacement.position;
            Vector3 midPointHorizontal = Vector3.Lerp(startPos, endPos, 0.5f);
            Vector3 controlPoint1 = midPointHorizontal + Vector3.up * hopHeight;
            Vector3 controlPoint2 = controlPoint1;

            float elapsedTime = 0f;

            while (elapsedTime < moveDurationPerTile)
            {
                float t = elapsedTime / moveDurationPerTile;
                playerPiece.transform.position = CalculateCubicBezierPoint(t, startPos, controlPoint1, controlPoint2, endPos);
                elapsedTime += Time.deltaTime;
                yield return null;
            }
            playerPiece.transform.position = endPos;
        }

        Debug.Log($"[Animator] Movement animation finished.");
        isAnimating = false;
        onComplete?.Invoke(); // Signal completion via the callback
    }

    private Vector3 CalculateCubicBezierPoint(float t, Vector3 p0, Vector3 p1, Vector3 p2, Vector3 p3)
    {
        float u = 1 - t; float tt = t * t; float uu = u * u; float uuu = uu * u; float ttt = tt * t;
        Vector3 p = uuu * p0; p += 3 * uu * t * p1; p += 3 * u * tt * p2; p += ttt * p3; return p;
    }
}