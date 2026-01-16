using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using DG.Tweening;

public class UIClickController : MonoBehaviour
{
    public Vector3 targetScale = new Vector3(1.5f, 1.5f, 1f);  // 拡大サイズ
    public float duration = 0.5f;       // 拡大・縮小にかかる時間
    public float interval = 1.0f;
    // Start is called before the first frame update
    void Start()
    {
        // 初期スケールを保存
        Vector3 originalScale = transform.localScale;

        // Sequenceを作成
        Sequence seq = DOTween.Sequence();

        seq.Append(transform.DOScale(targetScale, duration))
           .AppendInterval(interval)
           .Append(transform.DOScale(originalScale, duration))
           .AppendInterval(interval)
           .SetLoops(-1);  // 無限ループ
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}