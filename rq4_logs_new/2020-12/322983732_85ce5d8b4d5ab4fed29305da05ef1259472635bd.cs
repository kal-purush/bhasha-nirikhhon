using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Random = UnityEngine.Random;

public class EnemiesGenerator : MonoBehaviour
{

    public GameObject theEnemy;
    public int xPos;
    public int yPos;
    public int enemyCount;

    private void Start()
    {
        StartCoroutine(EnemyDrop());
    }

    IEnumerator EnemyDrop()
    {
        while (enemyCount<5)
        {
            xPos = Random.Range(1, 50);
            yPos = Random.Range(1, 30);
            Instantiate(theEnemy, new Vector3(xPos, yPos), Quaternion.identity);
            yield return new WaitForSeconds(0.1f);
            enemyCount += 1;
        }
    }
}