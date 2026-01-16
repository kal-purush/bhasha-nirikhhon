using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

namespace CompleteProject
{
    public class SceneSwitch : MonoBehaviour
    {
        public PlayerHealth Ph;
        private GameObject[] players;
        private GameObject currentplayers;
        Scene currentScene;
        string sceneName;
        public bool canSwitch;

        void Awake()
        {
            players = new GameObject[3];
            players[0] = GameObject.FindGameObjectWithTag("Ice");
            players[1] = GameObject.FindGameObjectWithTag("Fire");
            players[2] = GameObject.FindGameObjectWithTag("Earth");
            Scene currentScene = SceneManager.GetActiveScene();
            sceneName = currentScene.name;
            canSwitch = true;
        }

        // Update is called once per frame
        public void CheckSceneSwitch()
        {
            foreach (GameObject player in players)
            {
                if (player.GetComponent<PlayerHealth>().isDead)
                {
                    continue;
                }
                else if (!player.GetComponent<PlayerHealth>().isOnExit)
                {
                    canSwitch = false;
                }
            }
            if (canSwitch == true)
            {
                
                if (sceneName == "Room 1")
                {
                    SceneManager.LoadScene("Room 2");
                }
                else if (sceneName == "Room 2")
                {
                    SceneManager.LoadScene("Room 1");
                }
            }
        }
    }
}