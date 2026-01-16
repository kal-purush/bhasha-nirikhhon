using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BossFightManager : MonoBehaviour
{
    public GameObject[] bossObjects;
    //public int[][] spawnPosition;
    public int[] spawnPositionX;
    public int[] spawnPositionY;
    public float[] spawnTimes;

    private float time = 0;
    private int counter = 0;
    private float timerMax = 0;

    private void Start()
    {
        foreach (float s in spawnTimes) {
            if (s > timerMax)
            {
                timerMax = s;
            }
        }
    }

    void Update()
    {

        foreach (double s in spawnTimes) {
            if (time >= s) {
                Vector2 spawnPos = new Vector2(spawnPositionX[counter], spawnPositionY[counter]);
                Instantiate(bossObjects[counter], spawnPos, Quaternion.identity);
            }

            counter++;
        }
        counter = 0;

        if (time >= timerMax)
        {
            time = 0;
        } else
        {
            time += Time.deltaTime;
        }
    }
}