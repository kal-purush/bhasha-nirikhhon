using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class SceneChange : Collideable
{
    public string[] sceneNames;
    protected override void OnCollide(Collider2D coll)
    {
        if (coll.name == "Player_01")
        {
            //Teleport the player
            //To add later: GameManager.instance.SaveState();
            string sceneName = sceneNames[Random.Range(0, sceneNames.Length)];
            UnityEngine.SceneManagement.SceneManager.LoadScene(sceneName);
        }
    }
}