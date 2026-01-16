using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEditor.SceneManagement;

[System.Serializable]
public class PlayerData
{
    public int maxHealth;
    public int currentHealth;
    public float[] position;
    public int level;

    public PlayerData(Player player)
    {
        level = player.currentScene;
        maxHealth = player.maxHealth;
        currentHealth = player.currentHealth;
        position = new float[3];
        position[0] = player.transform.position.x;
        position[1] = player.transform.position.y;
        position[2] = player.transform.position.z;
    }
}