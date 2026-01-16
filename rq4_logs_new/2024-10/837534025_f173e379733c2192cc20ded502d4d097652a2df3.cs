using System;
using System.Collections;
using System.Collections.Generic;
using Characters.Defenders;
using Characters.Player;
using UnityEngine;

public class ProceduralEnemySpawner : MonoBehaviour
{
    public GameManager gameManager;
    public float currentRound;
    public float playerHealth;
    public GameObject detector;
    public detectTowers[] Lanes;
    public int[] Lanetowercount;
    

    private void Start()
    {
        gameManager = GameManager.Instance;

        for (int i = 0; i < 3; i++)
        {
           Lanes[i] = Instantiate(detector, gameManager._enemySpawnManager.spawnPoints[i], Quaternion.identity).GetComponent<detectTowers>();
        }
        
        
        // Initialize procedural parameters
        currentRound = gameManager.currentRound;
        playerHealth = gameManager.player.currentHealth;
        
       
        StartProceduralRound();
    }

    // Start a new round with procedural adjustments
    public void StartProceduralRound()
    {
        UpdatePlayerMetrics();
        
        CategorizeTowersByLane();
        
        AdjustSpawnParameters();

        gameManager.StartRound();
    }

    // Categorize towers by lane to adjust enemy spawning
    private void CategorizeTowersByLane()
    {
        for (int i = 0; i < Lanes.Length; i++)
        {
            Lanetowercount[i] = Lanes[i].towers.Count;
        }

        UpdatePlayerMetrics();
    }

   

    // Adjust spawn parameters based on towers in each lane
    private void AdjustSpawnParameters()
    {
        foreach (var lane in laneTowers.Keys)
        {
            List<GameObject> towersInLane = laneTowers[lane];
            bool hasBombTower = false;
            bool hasBasicTower = false;
            bool hasBuffTower = false;

             Check what types of towers are present in the lane
            foreach (var tower in towersInLane)
            {
                TowerController towerController = tower.GetComponent<TowerController>();

                if (towerController.isBombTower)
                    hasBombTower = true;
                if (towerController.isBasicTower)
                    hasBasicTower = true;
                if (towerController.isBuffTower)
                    hasBuffTower = true;
            }

            // Adjust spawns based on tower presence
            if (hasBombTower)
            {
                // Spawn more basic enemies in this lane, since they're weak to bombs
                SpawnEnemiesForLane(lane, "basic");
            }
            else if (hasBasicTower)
            {
                // Spawn more ranged enemies, weak to basic attacks
                SpawnEnemiesForLane(lane, "ranged");
            }
            else if (hasBuffTower)
            {
                // Spawn more tanks, weak to buffed attacks
                SpawnEnemiesForLane(lane, "tank");
            }
        }
    }

    // Spawns enemies for a specific lane based on tower weaknesses
    private void SpawnEnemiesForLane(int lane, string enemyTypeWeakness)
    {
        // You can extend this to control enemy counts, spawn rate, etc.
        switch (enemyTypeWeakness)
        {
            case "basic":
                enemySpawnManager.objToSpwn = 0;  // Assuming basic enemies are at index 0
                break;
            case "ranged":
                enemySpawnManager.objToSpwn = 1;  // Assuming ranged enemies are at index 1
                break;
            case "tank":
                enemySpawnManager.objToSpwn = 2;  // Assuming tanks are at index 2
                break;
        }

        // Use the enemySpawnManager to spawn enemies for the selected lane
        enemySpawnManager.spawnPoint = lane;  // Use the lane to set the spawn point
        StartCoroutine(enemySpawnManager.SpawnObject());
    }

    // Updates player metrics and dynamically changes difficulty
    public void UpdatePlayerMetrics()
    {
        currentRound = gameManager.currentRound;
        playerHealth = gameManager.player.currentHealth;

        // Apply spawn adjustments based on updated metrics
        AdjustSpawnParameters();
    }
}