using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class ArrowImageMove : MonoBehaviour
{
    [SerializeField] float speed = 3f;
    [SerializeField] float verticalMoveRange = 10f;

    float verticalMoveRangeHalf => verticalMoveRange / 2;

    Vector3 defaultPos;
    RectTransform rectTransform;

    protected void StartUp()
    {
        rectTransform = GetComponent<RectTransform>();
        defaultPos = rectTransform.position;
    }

    /// <summary>
    /// X方向にverticalMoveRange/2分動き、元に戻るを繰り返す
    /// </summary>
    protected void OnVerticalMove()
    {
        var min = defaultPos.x - verticalMoveRangeHalf;
        var max = defaultPos.x + verticalMoveRangeHalf;
        var move = rectTransform.position;

        move.x += Time.deltaTime * speed;
        move.x = Mathf.Clamp(move.x, min, max);

        if (move.x == max) move.x = min;
        else if (move.x == min) move.x = max;

        rectTransform.position = move;
    }

    /// <summary>
    /// 元の座標に戻す
    /// </summary>
    protected void OnDefaultPos()
    {
        rectTransform.position = defaultPos;
    }
}