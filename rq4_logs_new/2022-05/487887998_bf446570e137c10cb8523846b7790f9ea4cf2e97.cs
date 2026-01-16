using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using Photon.Pun;
using TMPro;


public class CreateAndJoinRooms : MonoBehaviourPunCallbacks
{

    public TMP_InputField createInput;
    public TMP_InputField joinInput;
    public TMP_InputField nicknameInput;


    public void CreateRoom()
    {
        PhotonNetwork.NickName = nicknameInput.text;
        PhotonNetwork.CreateRoom(createInput.text);
    }

    public void JoinRoom()
    {
        PhotonNetwork.NickName = nicknameInput.text;
        PhotonNetwork.JoinRoom(joinInput.text);
        
        
    }

    public override void OnJoinedRoom()
    {
        PhotonNetwork.LoadLevel("Game");
    }



}