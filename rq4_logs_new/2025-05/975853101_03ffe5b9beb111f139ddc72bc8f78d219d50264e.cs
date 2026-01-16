using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using DG.Tweening;

public class BaikinCOntroller_Hara : MonoBehaviour
{
    public float moveDistance = 1f;
    public float duration = 1f;

    // Start is called before the first frame update
    void Start()
    {
        transform.DOMoveY(transform.position.y + moveDistance, duration)
            .SetLoops(-1, LoopType.Yoyo) // 無限ループ、行き来する
            .SetEase(Ease.InOutSine);
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}