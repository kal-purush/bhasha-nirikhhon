using UnityEngine;
using Photon;
using RTS;
 
public class SpecificMatchMaker : Photon.PunBehaviour
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
        GUILayout.Label(PhotonNetwork.connectionStateDetailed.ToString() + " Player : " + PhotonNetwork.player.ID);
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
		int playerID = PhotonNetwork.player.ID;

		// Initialize player
		Vector3 playerPosition = new Vector3(10, 0, 10);
		GameObject playerObject = PhotonNetwork.Instantiate("Player", playerPosition, Quaternion.identity, 0); 
		Player myPlayer = playerObject.GetComponent<Player>();
		myPlayer.name = "MyNetworkPlayer" + playerID; // TODO must come from a GUI


		// Update name of children
		playerObject.GetComponentInChildren<Units>().name += playerID;
		playerObject.GetComponentInChildren<Buildings>().name += playerID;
		playerObject.GetComponentInChildren<RallyPoint>().name += playerID;


		// Enable the scripts related to the player
	 	myPlayer.transform.GetComponent<UserInput>().enabled = true;
	 	myPlayer.transform.GetComponent<GUIManager>().enabled = true;

		// Get the Units object
		Units units = playerObject.GetComponentInChildren< Units >();

		// Create units (and sending the name of the parent for those units)
		object[] data = new object[1];
		data[0] = units.name;			// parent name

		GameObject builder1 = PhotonNetwork.Instantiate("Builder", playerPosition, Quaternion.identity, 0, data);
		builder1.name = "Builder" + playerID;
		builder1.transform.parent = units.transform;

		GameObject tank1 = PhotonNetwork.Instantiate("Tank", playerPosition, Quaternion.identity, 0, data);
		tank1.name = "Tank" + playerID;
		tank1.transform.parent = units.transform;

		/*
		tank1.name = tank1.GetInstanceID().ToString();
		Debug.Log("Instance ID  : " + tank1.GetInstanceID());
        Debug.Log("Ovject : " + GameObject.Find(tank1.GetInstanceID().ToString()) );
	*/


	}
}