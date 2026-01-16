using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class EnemyQueue {

    public int maxQueueLength = 9;
    private static Queue<GameObject> enemiesQueue;
    public static GameObject currentEnemy;
    private static EnemyWatchdog watchdog;

    public EnemyQueue(EnemyWatchdog enemyWatchdog)
    {
        enemiesQueue = new Queue<GameObject>();
        watchdog = enemyWatchdog;
    }

    public void putEnemy()
    {
        currentEnemy = watchdog.pickEnemy();
        if (enemiesQueue.Count <= maxQueueLength)
        {
            enemiesQueue.Enqueue(currentEnemy);
        }
        else
        {
            System.Diagnostics.Debug.WriteLine("The queue is full");
        }
    }
   
    /*
    private void fightEnemy()
    {
         
        if (state != encounterState.None)
        {
            return;
        }

        else if (enemiesQueue.Count != 0)
        {
            currentEnemy = enemiesQueue.Dequeue();
            state = encounterState.Fight;
            UnityEngine.SceneManagement.SceneManager.LoadScene(Player.FIGHTING_LEVEL);
        }
        else
        {
            print("The Queue is empty");
        }
    }
    */

    private void goToWalkingScreen()
    {
        UnityEngine.SceneManagement.SceneManager.LoadScene(Player.WALKING_LEVEL);
    }

    public GameObject removeEnemy()
    {      
        if(IsEmpty())
        {
            return watchdog.pickEnemy();
        }
        else return enemiesQueue.Dequeue();
    }

    public bool IsEmpty()
    {
        if (enemiesQueue.Count == 0)
        {
            return true;
        }
        else
        {
            return false;
        }
    }

    public int getSize()
    {
        return enemiesQueue.Count;
    }
}