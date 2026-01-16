using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class TimeManager : MonoBehaviour
{
    /// <summary>タイムの状況</summary>
    TimeState m_timeState = TimeState.Stop;
    /// <summary>クリアタイムを計測</summary>
    float m_clearTime = 0f;
    /// <summary>タイム表示のテキスト</summary>
    [SerializeField] Text m_timerText = null;

    /// <summary>
    /// タイム管理
    /// </summary>
    public void TimeNow()
    {
        switch (m_timeState)
        {
            case TimeState.Addition:
                m_clearTime += Time.deltaTime;
                break;
            case TimeState.Stop:
                break;
            case TimeState.Reset:
                m_clearTime = 0f;
                break;
        }
        //テキストにタイムを表示させる
        if (m_timerText)
        {
            m_timerText.text = string.Format("{0:000.00}", m_clearTime);
        }
    }

    /// <summary>タイムを加算する</summary>
    public void TimerStart()
    {
        m_timeState = TimeState.Addition;
    }

    /// <summary>タイムを停止</summary>
    public void TimerStop()
    {
        m_timeState = TimeState.Stop;
    }

    /// <summary>タイムを初期化</summary>
    public void TimerReset()
    {
        m_clearTime = 0f;
    }

    /// <summary>
    /// タイム状態
    /// </summary>
    enum TimeState
    {
        /// <summary>タイムを加算</summary>
        Addition,
        /// <summary>タイムを止める</summary>
        Stop,
        /// <summary>タイムをリセット</summary>
        Reset
    }
}