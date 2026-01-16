using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UnityEngine;
using UnityEngine.Networking;

namespace Assets.scripts
{
    /*
     * Network Manager for intitiating Client/Host communications
     */
    class OONetworkManager : NetworkManager
    {
        public override void OnServerAddPlayer(NetworkConnection conn, short playerControllerId)
        {
            GameObject player = (GameObject)Instantiate(playerPrefab, Vector3.zero, Quaternion.identity);
            player.GetComponent<Player>().plugCoordinates = new PlugCoordinates();
            player.GetComponent<Player>().playerId = playerControllerId;
            NetworkServer.AddPlayerForConnection(conn, player, playerControllerId);
        }
    }
}