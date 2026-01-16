using Photon.Pun;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using Photon.Realtime;

//ランダムに縛りの内容を選ぶ(9/7 岩下)

public class ShibariRandomManager : MonoBehaviourPunCallbacks
{
    public Text RespondentTextBox;
    public Text QuestionerTextBox;
    public string[] ShibariWord;//要素数 : 縛りの数
    private int x;
    private int MasterX;
    private bool shibari_judge;
    private Room pc;

    // Start is called before the first frame update
    void Start()
    {
        RespondentTextBox.text = "";//テキストの初期化
        QuestionerTextBox.text = "";//テキストの初期化
        //縛りの内容を記述(余裕あればインスペクターで変更できるようにします)

        x = 0;
        shibari_judge = true;

        pc = PhotonNetwork.CurrentRoom;
    }

    // Update is called once per frame
    void Update()
    {
        if (pc.PlayerCount == pc.MaxPlayers && !v.turn_frag)
        {
            if (shibari_judge)
            {
                //一ターンで一回だけ実行
                shibari_judge = false;
                Debug.Log("ランダムに縛りをつけます");
                //マスターの変数を全員で共有する方法(備忘録)
                if (PhotonNetwork.IsMasterClient)
                {
                    MasterX = Random.Range(0, ShibariWord.Length);
                    photonView.RPC(nameof(RandomWord), RpcTarget.All, MasterX);
                }
            }
            RespondentTextBox.text = ShibariWord[x]; //テキストの上書き
            QuestionerTextBox.text = ShibariWord[x]; //テキストの上書き
        }
        else
        {
            shibari_judge = true;
        }
    }

    //マスターがランダムに選んだ値を全員で共有
    [PunRPC]
    private void RandomWord(int localX)
    {
        Debug.Log(localX);
        x = localX;
    }
}