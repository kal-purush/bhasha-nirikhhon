using UnityEngine;
using System.Collections;

namespace UnityStandardAssets.Network
{
    public class CatanLobbyHook : LobbyHook
    {
        public override void OnLobbyServerSceneLoadedForPlayer(UnityEngine.Networking.NetworkManager manager, GameObject lobbyPlayer, GameObject gamePlayer)
        {
            LobbyPlayer l = lobbyPlayer.GetComponent<LobbyPlayer>();

            ScriptPlayer player = gamePlayer.GetComponent<ScriptPlayer>();

            player.PlayerName = l.name;
            //player.color = l.playerColor;
            //paddle.playerName = l.playerName;

            //PongManager.AddPlayer(paddle);
        }
    }
}