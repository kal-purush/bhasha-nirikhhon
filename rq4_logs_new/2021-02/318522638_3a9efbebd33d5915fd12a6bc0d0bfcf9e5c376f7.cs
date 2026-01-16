using System.Collections.Generic;
using UnityEngine;

public class HostTeleportPartyToSelfTest : MonoBehaviour
{
    private void Start()
    {
        Debug.Log("(Tip: Don't hit alt) Host can pull team with F4");
    }

    private void Update()
    {
        if (Input.GetKeyDown(KeyCode.F4))
        {
            List<Player> players = PlayersDict.Instance.Players;
            for (int i = 0; i < players.Count; i++)
            {
                if (players[i] == Player.LocalPlayer)
                    continue;

                players[i].SmoothSync.teleportAnyObjectFromServer(Player.LocalPlayer.transform.position, Quaternion.identity, Vector3.one);
            }
        }
    }
}