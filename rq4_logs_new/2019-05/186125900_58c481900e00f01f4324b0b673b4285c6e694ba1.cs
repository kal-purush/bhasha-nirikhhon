using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class GameManager : MonoBehaviour
{

    public static GameManager instance = null; //crates a single instance of gamemanager
    public GameObject spawnPoint;
    public GameObject[] enemies; //currentely doesnt need to be an array because only 1 enemy
    public int maxEnemiesOnScreen; // how many on the screen
    public int totalEnemies; //how many enemies in the wave
    public int enemiesPerSpawn; //how many spawn per time

    private int enemiesOnScreen; //how many are there now


    void Awake()
    {
       
        if (instance==null)
        {
            instance = this;
        }
        else if (instance!=this)
        {
            Destroy(gameObject);
        }
        else
        {
            DontDestroyOnLoad(gameObject);
        }
    }

    // Start is called before the first frame update
    void Start()
    {
        EnemySpawn();
        
    }

    void EnemySpawn()
    {
        if (enemiesPerSpawn >0 && enemiesOnScreen<totalEnemies)
        {
            for (int i = 0; i < enemiesPerSpawn; i++)
            {
                if (enemiesOnScreen<maxEnemiesOnScreen)
                {
                    // needs to be cast as gameobject because Instantiate creates an object not a Gameobject
                    GameObject newEnemy = Instantiate(enemies[0]) as GameObject;

                    newEnemy.transform.position = spawnPoint.transform.position;
                    enemiesOnScreen += 1;
                }
            }
        }
    }
   
}