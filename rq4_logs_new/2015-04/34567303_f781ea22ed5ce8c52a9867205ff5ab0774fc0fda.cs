//Project EcliPse - Shenkar final project 2015.
//Gal Shalit, Yaniv Levi, David Faizulaev & Avishag Zehavi
using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class NetworkManager2 : Photon.MonoBehaviour {

	int whoAmI;
	public OVRCameraRig ovrCam;
	public Camera cam;//test cam
	spawnSpot[] spots;
	int groupId;
	GameObject Fighter;
	Camera[] displayCams;

	void getGroupId(){
		int num = PhotonNetwork.player.ID;
		//Debug.Log ("player.ID "+num);
		if (num % 2 == 0) {
			groupId = (num / 2) - 1;
			//return;
		} else {
			groupId = num / 2;
			//return;
		}
		Debug.Log (groupId);
	}

	public void ConnectAsPliot(){

		Debug.Log("ConnectAsPliot to PhotonServer");
		PhotonNetwork.ConnectUsingSettings("alpha");
		whoAmI = 0; //Pilot
	}

	public void ConnectAsGunner(){
		Debug.Log("ConnectAsGunner to PhotonServer");
		PhotonNetwork.ConnectUsingSettings("alpha");
		whoAmI = 1; //Gunner
	}

	void OnGUI(){
		GUILayout.Label (PhotonNetwork.connectionStateDetailed.ToString());
		GUILayout.Label ("Number of players in room "+PhotonNetwork.countOfPlayers.ToString());
		GUILayout.Label ("I R NUMBER " + PhotonNetwork.player.ID);
		if (!(Fighter == null)) {
			GUILayout.Label ("Number of players in room "+Fighter.transform.position.ToString());		
		}
	}

	void OnJoinedLobby(){
		Debug.Log("OnJoinedLobby");
		PhotonNetwork.JoinRandomRoom ();
	}
	void OnPhotonRandomJoinFailed(){
		PhotonNetwork.CreateRoom (null); // need to change room name to something with value
		Debug.Log("OnPhotonJoinRoomFailed");
	}
	void OnCreateRoom(){
		Debug.Log ("OnCreateRoom");
	}
	void OnJoinedRoom(){
		Debug.Log ("OnJoinedRoom");
		getGroupId ();
		Debug.Log("GroupId  "+groupId);

		//check number of connected players - if only 1 is connected (me) - wait until another player connects
		//then Instantiate Fighter - do only for 2 players.
		int MYID = PhotonNetwork.player.ID;
		Debug.Log ("MY ID IS" + MYID);

		if (MYID == 1) {
						Fighter = PhotonNetwork.Instantiate ("fighter", Vector3.zero, Quaternion.identity,/*groupId*/0);
						spots = Fighter.GetComponentsInChildren<spawnSpot> ();
						spawn ();
			
		} else {
			//Still need to find the existing Fighter GameObject
			spots = Fighter.GetComponentsInChildren<spawnSpot>();
			spawn ();
		}
		//Debug.Log ("Number of players in roon:" + PhotonNetwork.countOfPlayers);
		//Fighter = PhotonNetwork.Instantiate ("fighter", Vector3.zero, Quaternion.identity,/*groupId*/0);
		//spots = Fighter.GetComponentsInChildren<spawnSpot>();
		//spawn ();
	}

	void spawn(){

		displayCams = Fighter.GetComponentsInChildren<Camera> ();
		displayCams [0].enabled = false;
		displayCams [1].enabled = false;
		displayCams [2].enabled = false;
		//Fighter.GetComponents<d
		if (spots == null) {
			Debug.LogError ("unable spawn to a spot, spots = null");
		}
		Debug.Log ("My role(0 pilot 1 gunner) is: "+whoAmI);
		spawnSpot mySpot = spots [whoAmI];

		cam.transform.position = mySpot.transform.position;
		cam.transform.rotation = mySpot.transform.rotation;
		
		cam.transform.position = new Vector3 (ovrCam.transform.position.x, 5.319f, ovrCam.transform.position.z);//need to change the spawn height and then to remove this line
		Fighter.GetComponent<networkFighter> ().myCam = cam;
		Fighter.GetComponent<networkFighter> ().displayCams = displayCams;
		cam.GetComponent<CameraFollow> ().SetTarget (mySpot.transform);

		/*oculus section
		ovrCam.transform.position = mySpot.transform.position;
		ovrCam.transform.rotation = mySpot.transform.rotation;

		ovrCam.transform.position = new Vector3 (ovrCam.transform.position.x, 5.319f, ovrCam.transform.position.z);//need to change the spawn height and then to remove this line
		Fighter.GetComponent<networkFighter> ().myCam = ovrCam;
		ovrCam.GetComponent<CameraFollow> ().SetTarget (mySpot.transform);
*/
	}

}