using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnemyPosition : MonoBehaviour
{
    public int enemyOnLevel;
    private GameObject[] enemy;
    private int[] usedNum;
    private int enemyRemoveCount = 0, randomNumber;
    private System.Random key = new System.Random();
    private bool nextNum = false;
    // Start is called before the first frame update
    void Start()
    {
        enemy = GameObject.FindGameObjectsWithTag("Enemy");
        enemyRemoveCount = enemy.Length - enemyOnLevel;
        print(enemyRemoveCount);
        usedNum = new int[enemyRemoveCount];
        for (int i = 0; i < usedNum.Length; i++)
        {
            usedNum[i] = -1;
        }
        RevomeEnemy();
    }
    private void RevomeEnemy()
    {
        for (int i = 0; i < enemyRemoveCount; i++)
        {
            print(i);
            nextNum = false;
            while (nextNum == false)
            {
                randomNumber = key.Next(0, enemy.Length - 1);
                for (int j = 0; j < usedNum.Length; j++)
                {
                    if (usedNum[j] == randomNumber)
                    {
                        break;
                    }
                    else if (usedNum[j] == -1)
                    {
                        usedNum[i] = randomNumber;
                        print(randomNumber);
                        Destroy(enemy[randomNumber]);
                        nextNum = true;
                    }
                }
            }
        }
    }
}