using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Playables;
public class GameStart : MonoBehaviour
{
    /// <summary>
    /// ゲーム開始時に呼ぶTimeline
    /// </summary>
    [SerializeField] PlayableDirector m_gameStartTimeline;
    /// <summary>
    /// プレイヤーが動けるかどうかを管理する変数
    /// </summary>
    [SerializeField]private bool m_playerOperation = false;
    /// <summary>
    /// プレイヤーが動けるかどうかを管理する変数のプロパティ
    /// </summary>
    public bool PlayerOperation 
    {
        get { return m_playerOperation; }
        set { m_playerOperation = value; }
    
    }
    /// <summary>
    /// セットされたTimelineを再生する関数
    /// </summary>
    void GameStartTime()
    {
        Debug.Log("Timelineを再生");
        m_gameStartTimeline.Play();
    }

}