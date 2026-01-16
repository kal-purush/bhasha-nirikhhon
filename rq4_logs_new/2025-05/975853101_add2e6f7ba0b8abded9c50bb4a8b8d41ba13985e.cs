using UnityEngine;
using TMPro;
using DG.Tweening;

public class GameClearTextEffect : MonoBehaviour
{
   [Header("References")]
    public TextMeshProUGUI clearText;
    public AudioClip clearSE; // AudioSource は削除
    public AudioClip clearSE2; // AudioSource は削除

    [Header("Pulse Settings")]
    public float pulseScaleAmount = 0.1f;  // 拡大縮小の振れ幅
    public float pulseSpeed = 1.5f;        // 拡大縮小の速度

    private Tween pulseTween;

    void Start()
    {
    }

    public void PlayClearEffect()
    {
        clearText.gameObject.SetActive(true);
        clearText.transform.localScale = Vector3.zero;
        clearText.transform.rotation = Quaternion.identity;

       if (clearSE != null)
        {
            Vector3 soundPosition = Camera.main != null ? Camera.main.transform.position : Vector3.zero;
            AudioSource.PlayClipAtPoint(clearSE, soundPosition);
            AudioSource.PlayClipAtPoint(clearSE2, soundPosition);
        }

        // 登場アニメーション（回転＋拡大）
        Sequence seq = DOTween.Sequence();
        seq.Append(clearText.transform.DOScale(1.2f, 0.5f).SetEase(Ease.OutBack));
        seq.Join(clearText.transform.DOLocalRotate(new Vector3(0, 0, 720), 0.5f, RotateMode.FastBeyond360));
        seq.Append(clearText.transform.DOScale(1.0f, 0.2f).SetEase(Ease.OutBounce));
        seq.OnComplete(StartPulseEffect); // アニメ後にPulse開始
    }

    private void StartPulseEffect()
    {
        pulseTween = clearText.transform
            .DOScale(new Vector3(1 + pulseScaleAmount, 1 + pulseScaleAmount, 1), pulseSpeed)
            .SetLoops(-1, LoopType.Yoyo)
            .SetEase(Ease.InOutSine);
    }

    public void StopPulseEffect()
    {
        if (pulseTween != null && pulseTween.IsActive())
            pulseTween.Kill();
    }
}