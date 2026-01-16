using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerPostionBetweenScenes : ScriptableObject
{
    public Transform player;
    public Dictionary<string, Vector2> startingPosition = new();

    public void SavePosition(string sceneName)
    {
        if (startingPosition.ContainsKey(sceneName))
        {
            startingPosition[sceneName] = player.position;
        }
        else
        {
            startingPosition.Add(sceneName, player.position);
        }
    }
}