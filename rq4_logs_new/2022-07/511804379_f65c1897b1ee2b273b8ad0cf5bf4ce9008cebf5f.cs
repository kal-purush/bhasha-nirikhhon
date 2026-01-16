using System;
using System.Collections.Generic;
using System.Linq;
using ExitGames.Client.Photon;
using Photon.Pun;
using Photon.Pun.Demo.Cockpit.Forms;
using Photon.Realtime;
using TMPro;
using UnityEngine;
using UnityEngine.UI;

public class ConnectAndJoinRandomLobby : MonoBehaviour, IConnectionCallbacks, IMatchmakingCallbacks, ILobbyCallbacks
{
    [SerializeField] private ServerSettings _serverSettings;
    [SerializeField] private TMP_Text _stateUiText;
    [SerializeField] private Button _buttonCreateAndJoinRoom;
    [SerializeField] private Button _makeRoomPrivate;
    [SerializeField] private Button _joinByName;
    [SerializeField] private Button _leaveRoom;
    [SerializeField] private MenuSelectRoom _menuSelectRoom;
    [SerializeField] private RectTransform _content;
    [SerializeField] private TMP_InputField _joinByNameInput;

    private const string AI_KEY = "ai";
    private const string PLAYERS_KEY = "rp";
    
    private const string EXP_KEY = "C0";
    private const string MAP_KEY = "C1";
    
    private TypedLobby _sqlLobby = new TypedLobby("sqlLobby", LobbyType.SqlLobby);
    private List<MenuSelectRoom> _menuSelectRooms = new List<MenuSelectRoom>();
    private LoadBalancingClient _lbc;

    private void Start()
    {
        _lbc = new LoadBalancingClient();
        _lbc.AddCallbackTarget(this);
        _lbc.ConnectUsingSettings(_serverSettings.AppSettings);
        _buttonCreateAndJoinRoom.onClick.AddListener(CreateRoom);
        _makeRoomPrivate.onClick.AddListener(CreateRoomPrivate);
        _joinByName.onClick.AddListener(JoinByNameRoom);
        _leaveRoom.onClick.AddListener((() =>
        {
            _lbc.OpLeaveRoom(false);
        }));
        
    }

    private void JoinByNameRoom()
    {
        _lbc.OpJoinRoom(new EnterRoomParams
        {
            RoomName = _joinByNameInput.text
        });
    }
    private void CreateRoomPrivate()
    {
        _lbc.CurrentRoom.IsOpen = false;
        _lbc.CurrentRoom.IsVisible = false;
    }

    private void CreateRoom()
    {
        var roomOptions = new RoomOptions
        {
            MaxPlayers = 4,
            IsVisible = true
        };
        
        var enterRoomParams = new EnterRoomParams
        {
            RoomName = "NewRoom",
            RoomOptions = roomOptions,
        };
        _lbc.OpCreateRoom(enterRoomParams);
    }

    private void OnDestroy()
    {
        _lbc.RemoveCallbackTarget(this);
        _buttonCreateAndJoinRoom.onClick.RemoveAllListeners();
        _joinByName.onClick.RemoveAllListeners();
        _leaveRoom.onClick.RemoveAllListeners();
    }

    private void Update()
    {
        if (_lbc == null)
            return;
        
        _lbc.Service();
        
        var state = _lbc.State.ToString();
        _stateUiText.text = $"State: {state}, userID: {_lbc.UserId}";
    }

    public void OnConnected()
    {
        Debug.Log("OnConnected");
    }

    public void OnConnectedToMaster()
    {
        Debug.Log("OnConnectToMaster");
        _lbc.OpJoinLobby(new TypedLobby("customLobby", LobbyType.Default));
    }

    public void OnDisconnected(DisconnectCause cause)
    {
       
    }

    public void OnRegionListReceived(RegionHandler regionHandler)
    {
        
    }

    public void OnCustomAuthenticationResponse(Dictionary<string, object> data)
    {
        
    }

    public void OnCustomAuthenticationFailed(string debugMessage)
    {
        
    }

    public void OnFriendListUpdate(List<FriendInfo> friendList)
    {
        
    }

    public void OnCreatedRoom()
    {
        
    }

    public void OnCreateRoomFailed(short returnCode, string message)
    {
        
    }

    public void OnJoinedRoom()
    {
        Debug.Log("OnJoinedRoom");
    }

    public void OnJoinRoomFailed(short returnCode, string message)
    {
        Debug.Log("Wrong room name");
    }

    public void OnJoinRandomFailed(short returnCode, string message)
    {
        Debug.Log("OnJoinRandomFailed");
    }

    public void OnLeftRoom()
    {
        
    }

    public void OnJoinedLobby()
    {
        _buttonCreateAndJoinRoom.gameObject.SetActive(true);
    }

    public void OnLeftLobby()
    {
        
    }

    public void OnRoomListUpdate(List<RoomInfo> roomList)
    {
        if (roomList == null)
            return;
        if (_menuSelectRooms.Capacity > 0)
        {
            foreach (var menuSelectRoom in _menuSelectRooms)
            {
                if (menuSelectRoom != null)
                {
                    Destroy(menuSelectRoom.gameObject);
                }
            }
        }
        foreach (var room in roomList)
        {
            var field = Instantiate(_menuSelectRoom, _content);
            _menuSelectRooms.Add(field);
            field.SetRoomName(room);
            field.Join += JoinRoomAction;
        }
    }

    private void JoinRoomAction(string var)
    {
        _lbc.OpJoinRoom(new EnterRoomParams
        {
            RoomName = var
        });
    }

    public void OnLobbyStatisticsUpdate(List<TypedLobbyInfo> lobbyStatistics)
    {
        
    }
}