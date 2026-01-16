using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using Photon.Pun;
using Photon.Realtime;
using Hashtable = ExitGames.Client.Photon.Hashtable;

public class ReadyBtn : MonoBehaviourPunCallbacks, IPunObservable
{
    public PhotonView pv;            // Ready 텍스트 포톤뷰
    Hashtable temp;                  // 임시 해시 테이블
    string isReady;                  // Ready 여부 변수

    public void Start()
    {
        // Canvas 하위로 이동
        transform.SetParent(GameObject.Find("Canvas").GetComponent<Transform>(), false);
        ColorChange(isReady);
    }

    // ready 버튼 클릭(ex) 0번 플레이어가 0번 Ready를 눌러야만 로컬에서 색깔 바꿈)
    public void Update()
    {
        if(GameObject.Find("Ready").GetComponent<ReadyClick>().isClick)  // 본인 Ready 버튼 isClick X
        {
            if (pv.IsMine)
            {
                temp = PhotonNetwork.LocalPlayer.CustomProperties;

                if(temp.ContainsKey("Ready"))
                {
                    if (((string)temp["Ready"]).Equals("Yes"))
                    {
                        temp["Ready"] = "No";                                 // 직접 대입 X
                        PhotonNetwork.LocalPlayer.SetCustomProperties(temp);  // PunRPC에서 사용X
                        pv.RPC("ColorChange", RpcTarget.AllBufferedViaServer, "No");
                    }
                    else
                    {
                        temp["Ready"] = "Yes";
                        PhotonNetwork.LocalPlayer.SetCustomProperties(temp);
                        pv.RPC("ColorChange", RpcTarget.AllBufferedViaServer, "Yes");
                    }
                }
            }
            GameObject.Find("Ready").GetComponent<ReadyClick>().isClick = false;
        }
    }
    
    // Ready 텍스트 프로퍼티에 맞춰 변경
    [PunRPC]
    void ColorChange(string value) // Localplayer와 PhotonView(pv) 같지 않음
    {
        isReady = value;

        if ((value == null) || (value.Equals("No")))
        {
            gameObject.GetComponent<Text>().text = "";
        }
        else
        {
            gameObject.GetComponent<Text>().text = "Ready";
        }
    }

    public void OnPhotonSerializeView(PhotonStream stream, PhotonMessageInfo info)
    {
        if (stream.IsWriting)
        {
            stream.SendNext(isReady);
        }
        else
        {
            this.isReady = (string)stream.ReceiveNext();
        }
    }
}