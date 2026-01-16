using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.UI;

public class PhotonNetworkManager : MonoBehaviour
{

    private List<GameObject> roomsPrefab = new List<GameObject>();
    public static PhotonNetworkManager instance = null;
    public GameObject RoomPrefab;
    public InputField RoomName;

    void Awake()
    {
        if (instance == null)
        {
            instance = this;
            DontDestroyOnLoad(gameObject.transform);
        }
        else if (instance != this)
        {
            Destroy(gameObject);
        }
    }

    // Update is called once per frame
    void Start()
    {
        PhotonNetwork.ConnectUsingSettings("1.0");
        PhotonNetwork.sendRate = 30;
        PhotonNetwork.sendRateOnSerialize = 30;

        SceneManager.sceneLoaded += (scene, loadscene) =>
        {
            if (SceneManager.GetActiveScene().name == "Projekt")
            {
                Spawn();
            }
        };
    }

    void Spawn()
    {
        List<string> availPlayers = new List<string>();
        availPlayers.Add("Archer");
        availPlayers.Add("Wizard");
        availPlayers.Add("Warrior");


        foreach (PhotonPlayer otherPlayer in PhotonNetwork.otherPlayers)
        {
            availPlayers.Remove(otherPlayer.NickName);
            Debug.Log("Found other player: " + otherPlayer.NickName);
        }
        if (availPlayers.Count > 0)
        {
            PhotonNetwork.player.NickName = availPlayers[0];
        }
        else
        {
            Debug.Log("No more players left");
            return;
        }
        Debug.Log("Let begin as " + PhotonNetwork.player.NickName);

        // Scan all player spawn points
        var spawnPoints = GameObject.FindGameObjectsWithTag("SpawnPoint");
        foreach (var spawnPoint in spawnPoints)
        {
            var spawnParams = spawnPoint.GetComponent<SpawnPointParameters>();

            // Spawn next player if available
            if (spawnParams.prefab.name == PhotonNetwork.player.NickName)
            {
                // Instantiate player
                var currentPlayer = (GameObject)PhotonNetwork.Instantiate(spawnParams.prefab.name, spawnPoint.transform.position, spawnPoint.transform.rotation, 0);
                currentPlayer.transform.FindChild("Camera").gameObject.SetActive(true);
                currentPlayer.GetComponent<PhotonView>().RequestOwnership();
                currentPlayer.transform.Find("Camera").tag = "MainCamera";
            }
        }
    }
    public void ButtonEvents(string ev)
    {
        switch (ev)
        {
            case "CreateRoom":
                if (PhotonNetwork.JoinLobby())
                {
                    Debug.Log("Create Room Click");
                    RoomOptions options = new RoomOptions();
                    options.MaxPlayers = 3;
                    PhotonNetwork.CreateRoom(RoomName.text, options, TypedLobby.Default);
                }
                break;
            case "Refresh":
                if (PhotonNetwork.JoinLobby())
                    RefreshList();
                break;

        }
    }
    void RefreshList()
    {
        if (roomsPrefab.Count > 0)
        {
            for (int i = 0; i < roomsPrefab.Count; i++)
            {
                Destroy(roomsPrefab[i]);
            }

            roomsPrefab.Clear();
        }

        for (int i = 0; i < PhotonNetwork.GetRoomList().Length; i++)
        {
            GameObject game = Instantiate(RoomPrefab);
            game.transform.SetParent(RoomPrefab.transform.parent);
            game.GetComponent<RectTransform>().localScale = RoomPrefab.GetComponent<RectTransform>().localScale;
            game.GetComponent<RectTransform>().position = new Vector3(RoomPrefab.GetComponent<RectTransform>().localPosition.x, RoomPrefab.GetComponent<RectTransform>().localPosition.y - (i * 50), RoomPrefab.GetComponent<RectTransform>().localPosition.z);
            game.transform.FindChild("Room_Name_Text").GetComponent<Text>().text = PhotonNetwork.GetRoomList()[i].Name;
            game.transform.FindChild("Player_Count").GetComponent<Text>().text = PhotonNetwork.GetRoomList()[i].PlayerCount + "/3";
            game.transform.FindChild("Join").GetComponent<Button>().onClick.AddListener(()=> { JoinRoom(game.transform.FindChild("Room_Name_Text").GetComponent<Text>().text); });
            game.SetActive(true);
        }


    }
    void JoinRoom(string Name)
    {
        bool availRoom = false;
        foreach (RoomInfo RI in PhotonNetwork.GetRoomList())
        {
            if(Name == RI.Name)
            {
                availRoom = true;
            }
            else
            {
                availRoom = false;
            }
        }

        if (availRoom)
        {
            PhotonNetwork.JoinRoom(Name);
        }
        else
        {
            Debug.Log("Fail room connection");
        }
    }

    private void OnGUI()
    {
        GUILayout.Label(PhotonNetwork.connectionStateDetailed.ToString());
    }

    void OnJoinedLobby()
    {
        Debug.Log("lobby join");
        Invoke("RefreshList", 0.1f);
    }
    void OnPhotonJoinRoomFailed()
    {
        Debug.Log("Join room fail");
    }
    void OnJoinedRoom()
    {
        Debug.Log("joined to room");
        SceneManager.LoadScene("Projekt");
        Debug.Log("Joined room");
  
    }

    void OnCreateRoom()
    {
        Debug.Log("Room Created");
    }
}