using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class GameManager : MonoBehaviour
{
    /// <summary>ゲームの状況</summary>
    GameState m_gameState = GameState.NonInitialized;
    /// <summary>タイムの状況</summary>
    TimeState m_timeState = TimeState.Stop;
    /// <summary>クリアタイムを計測</summary>
    float m_clearTime = 0f;
    /// <summary>穴掘りクラス</summary>
    DrillingMethod m_drillingMethod = null;
    /// <summary>タイム表示のテキスト</summary>
    [SerializeField] Text m_timerText = null;

    // Start is called before the first frame update
    void Start()
    {
        m_drillingMethod = GetComponent<DrillingMethod>();
    }

    // Update is called once per frame
    void Update()
    {
        switch (m_gameState)
        {
            case GameState.NonInitialized:
                Standby();
                break;
            case GameState.Initialized:
                GameSetUp();
                break;
            case GameState.InGame:
                TimeManager();
                break;
            case GameState.Pause:
                break;
        }
    }

    /// <summary>
    /// ゲーム準備
    /// </summary>
    public void Standby()
    {
        m_drillingMethod.MappingStart();
        m_gameState = GameState.Initialized;
    }


    /// <summary>
    /// ゲームをセットアップする
    /// </summary>
    void GameSetUp()
    {
        m_gameState = GameState.InGame;
    }

    /// <summary>
    /// タイム管理
    /// </summary>
    void TimeManager()
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
    public void PlayerStart()
    {
        m_timeState = TimeState.Addition;
    }

    /// <summary>タイムを停止</summary>
    public void TimerStop()
    {
        m_timeState = TimeState.Stop;
    }

    enum TimeState
    {
        /// <summary>タイムを加算</summary>
        Addition,
        /// <summary>タイムを止める</summary>
        Stop,
        /// <summary>タイムをリセット</summary>
        Reset
    }

    enum GameState
    {
        /// <summary>ゲーム初期化前</summary>
        NonInitialized,
        /// <summary>ゲーム初期化済み、ゲーム開始前</summary>
        Initialized,
        /// <summary>ゲーム中</summary>
        InGame,
        /// <summary>ポーズ中</summary>
        Pause
    }
}