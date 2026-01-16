using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class LobbyController : MonoBehaviour {

	public GameObject chatRoom;
	public GameObject lobbyRoom;
	public InputField roomKey;
	public InputField userName;
	public Text roomListText;



	public void CreateRoom(){
		FirebaseDatabaseFacade.Instance.CreateRoom ();
		GameManager.Instance.userName = userName.text;
		GoToChatRoom();
	}

	public void JoinRoom(){
		FirebaseDatabaseFacade.Instance.JoinRoom (roomKey.text);
		GameManager.Instance.userName = userName.text;
		GoToChatRoom();
	}

	public void ShowRoom(List<string> roomList){

		for (int i = 0; i < roomList.Count; i++) {
			roomListText.text += "" + roomList [i] + "\n";
		}

	}

	private void GoToChatRoom (){
		lobbyRoom.SetActive (false);
		chatRoom.SetActive (true);
	}
}