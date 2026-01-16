using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GlobalPlayerPosition : ScriptableObject
{
    [SerializeField] public GameObject player;
    [SerializeField] public static Vector3 PlayerPositionLocation;

    public void SavePosition()
    {
        PlayerPositionLocation = player.transform.position;
    }

    public void LoadPosition()
    {
        player.transform.position = PlayerPositionLocation;
    }

    public void SetPlayer(GameObject player)
    {
        this.player = player;
    }

    public GameObject GetPlayer()
    {
        return player;
    }

    public void SetPlayerPosition(Vector3 position)
    {
        player.transform.position = position;
    }
}