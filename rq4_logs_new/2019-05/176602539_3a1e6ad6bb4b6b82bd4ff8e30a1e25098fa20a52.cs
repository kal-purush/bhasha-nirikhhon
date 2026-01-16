using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

/* Class responsible for managing basic player statistics like playerLives etc. */
/*Author: Martyna Drabińska*/
public class PlayerStatistics : MonoBehaviour
{
    public static int currentLives;
    public static int playerLives = 10;
    public static int killedEnemies = 0;
    public Text livesQuantity;

    /*Author: Martyna Drabińska*/
    public void Start()
    {
        currentLives = playerLives;
    }

    /*Author: Martyna Drabińska*/
    public void TakeOneLive()
    {
        if (currentLives > 0)
        {
            currentLives--;
            livesQuantity.text = currentLives.ToString();
            if (EnemySpawner.CheckIfGameEnd())
            {
                Debug.Log("end");
                GameObject.Find("GameMaster").GetComponent<LevelManager>().GameEnd();
            }
        }
        else
        {
            GameObject.Find("GameMaster").GetComponent<LevelManager>().GameOver();
        }

    }

    /*Author: Martyna Drabińska*/
    public static void increaseKilledEnemies()
    {
        killedEnemies++;
        if (EnemySpawner.CheckIfGameEnd())
        {
            Debug.Log("end");
            GameObject.Find("GameMaster").GetComponent<LevelManager>().GameEnd();
        }
    }
}