using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class SpawnCoin : MonoBehaviour
{
    [Header("SPAWN AREA")]
    public GameObject[] SpawnPoints;

    [Header("TARGETS")]
    public GameObject Player;
    public GameObject[] CoinPrefab;

    private int coinCtr = 0;
    private int roomCtr = 0;

    void Start()
    {
        // Randomize the Room List so the order of spawn is different
        for (int i = 0; i < SpawnPoints.Length; i++) {
            GameObject temp = SpawnPoints[i];
            int rndIndex = Random.Range(i, SpawnPoints.Length);
            SpawnPoints[i] = SpawnPoints[rndIndex];
            SpawnPoints[rndIndex] = temp;
        }

        // Spawn the First Coin
        Instantiate(CoinPrefab[coinCtr], getVectors(SpawnPoints[roomCtr]), Quaternion.identity);
    }

    // Update is called once per frame
    void Update()
    {
        
    }
    
    private Vector3 getVectors(GameObject target) {
        Vector3 spawnCoord = target.GetComponent<RoomInfo>().center;
        return spawnCoord;
    }
}