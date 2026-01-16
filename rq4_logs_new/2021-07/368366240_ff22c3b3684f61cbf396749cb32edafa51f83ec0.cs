using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Photon.Pun;   // PhotonNetwork を使うため
using Photon.Realtime;  // RaiseEventOptions/ReceiverGroup を使うため
using ExitGames.Client.Photon;  // SendOptions を使うため

///// <summary>
///// イベントコード一覧
///// </summary>
//public enum EventCodes
//{
//    /// <summary>
//    /// playerが死んだ時のコード
//    /// </summary>
//    IDied = 150,
//    waiting = 151,

//}
public class GM : MonoBehaviour
{

    [SerializeField] NetworkTest networkTest = null;
    static GameManagerTest m_instance;
    PhotonView m_view = null;

    public const byte eventCode = 150;




    enum GameStatus
    {
        Playing = 1,
        IDied = 10,
        Game_Win = 11,
        Game_Lose = 12
    }
    [SerializeField] GameStatus gamestatus = GameStatus.Playing;

    int numberOfLivingPlayer = 2;

    GameManagerTest masterGameManager;
    List<GameManagerTest> gameManagerList = new List<GameManagerTest>();

    private void Awake()
    {
        //m_instance = this;
    }

    private void Start()
    {
        m_view = GetComponent<PhotonView>();
    }

    /// <summary>
    /// マスター（ホスト）のゲームマネージャーを設定する
    /// </summary>
    /// <param name="masterGameManager">マスターのゲームマネージャー</param>
    public void SetMasterGameManager(GameManagerTest masterGameManager)
    {
        this.masterGameManager = masterGameManager;
    }

    /// <summary>
    /// 他のプレイヤーのゲームマネージャーを自身のリストに追加する
    /// </summary>
    /// <param name="gameManager_Another"></param>
    public void AddAnotherGameManager(GameManagerTest gameManager_Another)
    {
        gameManagerList.Add(gameManager_Another);
        numberOfLivingPlayer++;
    }
    /// <summary>
    /// 呼び出しテスト
    /// </summary>
    public void Test()
    {
        GameEvent(EventCodes.IDied);

    }

    /// <summary>
    /// イベントを他のユーザーに送信する関数
    /// </summary>
    public static void GameEvent(EventCodes eventCode)
    {
        switch (eventCode)
        {
            case EventCodes.IDied:
                //m_instance.gamestatus = GameStatus.Game_Lose;
                Debug.Log("You Lose...");
                break;
            default:
                break;
        }

        m_instance.SendEvent((byte)eventCode);
    }

    /// <summary>
    /// 他のプレイヤーが死んだときに、自身で管理している残りプレイヤー数を減らし、
    /// もし自分しか生き残っていなかったら勝利する
    /// </summary>
    public void AnotherPlayerDied()
    {
        numberOfLivingPlayer--;
        if (numberOfLivingPlayer == 1 && gamestatus == GameStatus.Playing)      //生きているプレイヤーが1人かつ、自分がゲーム中の時
        {
            Debug.Log("You Win!!");
        }
    }

    /// <summary>
    /// イベント送信処理
    /// </summary>
    /// <param name="eventCode"></param>
    public void SendEvent(byte eventCode)
    {
        object[] content = new object[] { new Vector3(10.0f, 2.0f, 5.0f), 1, 2, 5, 10 }; // Array contains the target position and the IDs of the selected units
        RaiseEventOptions raiseEventOptions = new RaiseEventOptions { Receivers = ReceiverGroup.Others }; // You would have to set the Receivers to All in order to receive this event on the local client as well
        PhotonNetwork.RaiseEvent(eventCode, null, raiseEventOptions, SendOptions.SendReliable);
    }

    /// <summary>
    /// イベント送信処理
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="eventCode">コード</param>
    /// <param name="data">データ</param>
    public void SendEvent(byte eventCode, Object data)
    {
        RaiseEventOptions raiseEventOptions = new RaiseEventOptions { Receivers = ReceiverGroup.Others };
        PhotonNetwork.RaiseEvent(eventCode, data, raiseEventOptions, SendOptions.SendReliable);
    }

    /// <summary>
    /// イベント受信
    /// </summary>
    /// <param name="e">受け取るイベント</param>
    public void OnEvent(EventData e)
    {
        if (e.Code == (byte)EventCodes.IDied)     //こっちは死んだよって言われたら
        {
            AnotherPlayerDied();
        }
        else if (e.Code == (byte)EventCodes.waiting)
        {
            networkTest.CameraTargetSet();
        }

    }

}
