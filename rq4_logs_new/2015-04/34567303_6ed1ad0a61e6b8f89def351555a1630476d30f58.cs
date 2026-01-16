using UnityEngine;
using System.Collections;

public class newNetworkManager : MonoBehaviour {
	

	GameObject Fighter;
	
	void OnGUI(){
		GUILayout.Label (PhotonNetwork.connectionStateDetailed.ToString ());
		GUILayout.Label ("Number of players in room " + PhotonNetwork.countOfPlayers.ToString ());
		GUILayout.Label ("player ID " + PhotonNetwork.player.ID);
		if (!(Fighter == null)) {
			GUILayout.Label ("Number of players in room " + Fighter.transform.position.ToString ());		
		}
	}

	void Start () {
			PhotonNetwork.ConnectUsingSettings("alpha");
	}


		// Update is called once per frame
	void Update () {
		
	}

	void OnJoinedLobby(){
		Debug.Log("OnJoinedLobby");
		PhotonNetwork.JoinRandomRoom ();
	}
	void OnPhotonRandomJoinFailed(){
		Debug.Log("OnPhotonJoinRoomFailed");
		PhotonNetwork.CreateRoom (null); // need to change room name to something with value	
	}
	

	void OnJoinedRoom(){
		Fighter = PhotonNetwork.Instantiate ("fighter", Vector3.zero, Quaternion.identity,/*groupId*/0);
	}

	

	
}