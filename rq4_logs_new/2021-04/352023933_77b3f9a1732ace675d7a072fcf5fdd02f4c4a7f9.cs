using System.Collections;
using System.Collections.Generic;
using UnityEngine;

/// <summary>
/// 波板のアニメーション
/// </summary>
public class WaveAnimation : MonoBehaviour
{
    Animator m_animator = null;

    // Start is called before the first frame update
    void Start()
    {
        m_animator = GetComponent<Animator>();
    }

    /// <summary>
    /// ウェーブのアニメーションを再生します。
    /// </summary>
    public void WaveAnimaPlay()
    {
        m_animator.SetFloat("WaveFloat", 2f);
        m_animator.Play("WaveAnimation", 0, 0f);
    }

    /// <summary>
    /// ウェーブのアニメーションを逆再生します。
    /// </summary>
    public void WaveAnimaRePlay()
    {
        m_animator.SetFloat("WaveFloat", -2f);
        m_animator.Play("WaveAnimation");
    }
}