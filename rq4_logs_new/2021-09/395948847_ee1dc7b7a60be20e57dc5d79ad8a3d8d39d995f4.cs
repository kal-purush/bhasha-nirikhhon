using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
//Photon使うときは必ずオブジェクトにPhoton Viewをつけること！
using Photon.Pun;
using Photon.Realtime;

public class ok : MonoBehaviourPunCallbacks
{
    // Start is called before the first frame update
    public static bool clk = false;
    public int max;
    public void OnClickPlus()
    {
        if (!clk)
        {
            clk = true;
            v.count++;
            Debug.Log("v.count : " + v.count);
            //          実行するメソッド名     対象           引数
            photonView.RPC("DebugNum", RpcTarget.AllBuffered, v.count);
        }
    }


    [PunRPC]
    void DebugNum(int count)
    {
        v.count = count;
        Debug.Log(count);
    }
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}