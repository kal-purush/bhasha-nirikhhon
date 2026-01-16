using UnityEngine;
using Photon;
using RTS;
 
public class RandomMatchmaker : Photon.PunBehaviour
{
    // Use this for initialization
    void Start()
    {
    	// Set the log level of Photon
    	//PhotonNetwork.logLevel = PhotonLogLevel.Full;
        PhotonNetwork.ConnectUsingSettings("0.1");
    }
 
    void OnGUI()
    {
        GUILayout.Label(PhotonNetwork.connectionStateDetailed.ToString());
    }

    // TODO : this is called automatically because "Auto-join lobby" is checked. Should change that at a point.
    public override void OnJoinedLobby()
	{
    	PhotonNetwork.JoinRandomRoom();
	}

	// If the auto-join fails, it means that there is no room, and that one must be created.
	void OnPhotonRandomJoinFailed()
	{
	    Debug.Log("Can't join random room!");
	    PhotonNetwork.CreateRoom(null);
	}

	void OnJoinedRoom()
	{
		Debug.Log("Current player : " + PhotonNetwork.player.ID); 

	    Player player = GameObject.FindWithTag("Player").GetComponent<Player>();
	 	Debug.Log("Player : " + player);   
	    //this.Transform.parent gmonster.GetComponent<CharacterControl>();
	    //controller.enabled = true;
	    //CharacterCamera camera = monster.GetComponent<CharacterCamera>();
	    //camera.enabled = true;
	}
}