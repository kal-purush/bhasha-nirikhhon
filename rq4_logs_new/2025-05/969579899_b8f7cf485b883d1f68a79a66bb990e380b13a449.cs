using DG.Tweening;
using TMPro;
using UnityEngine;
using UnityEngine.UI;

public class ScreenLoading : BaseScreen
{
    [SerializeField] private Slider m_SliderLoading;
    [SerializeField] private TextMeshProUGUI m_TextLoading;
    [SerializeField] private GlobalConfig m_GlobalConfig;
    private Sequence m_DotSequence;
    private string m_CurrentDots = "";

    private void Start()
    {
        SetSliderLoading();
        AnimateDotsWithSequence();
    }

    private void SetSliderLoading()
    {
        m_SliderLoading.value = 0;
        m_SliderLoading.DOValue(m_SliderLoading.maxValue,m_GlobalConfig.timeFakeLoading).OnUpdate(() =>
        {
            m_TextLoading.text = $"Loading{m_CurrentDots} {Mathf.FloorToInt(m_SliderLoading.value * 100f)}%";
        })
        .OnComplete(() =>
        {
            m_TextLoading.text = $"Loading ... 100%";
            canvasGroup.DOFade(0f, m_GlobalConfig.timeHideCanvasGroup).OnComplete(() =>
            {
                if (UIManager.HasInstance)
                {
                    UIManager.Instance.ShowScreen<ScreenBoardSelection>();
                }
                m_DotSequence.Kill();
            });
        });
    }
    private void AnimateDotsWithSequence()
    {
        m_DotSequence = DOTween.Sequence();
        m_DotSequence.SetLoops(-1);
        m_DotSequence
            .AppendCallback(() => m_CurrentDots = ".")
            .AppendInterval(m_GlobalConfig.timeDoAnimationText)
            .AppendCallback(() => m_CurrentDots = "..")
            .AppendInterval(m_GlobalConfig.timeDoAnimationText)
            .AppendCallback(() => m_CurrentDots = "...")
            .AppendInterval(m_GlobalConfig.timeDoAnimationText);
    }    
}