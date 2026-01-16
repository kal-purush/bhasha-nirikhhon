using System.Collections.Generic;
using UnityEngine;
using UnityEngine.AI;

public class EnemySpawnerWithIndividualRange : MonoBehaviour
{
    [System.Serializable]
    public class Level
    {
        public string levelName; // Name of the level
        public Transform levelReference; // Reference for spawn area or predefined spawn points
        public GameObject cowardEnemyPrefab; // Prefab for coward enemy
        public GameObject meleeEnemyPrefab; // Prefab for melee enemy
        public int totalEnemiesToSpawn = 10; // Total number of enemies to instantiate
        public float spawnRange = 10f; // Range around spawn area to instantiate enemies
        public float cowardEnemyPercentage = 50f; // Percentage of coward enemies
    }

    public List<Level> levels; // List of levels
    public float spawnHeightOffset = 0.5f; // Height adjustment to keep enemies above terrain
    public float navMeshCheckRadius = 1.0f; // Radius to check valid NavMesh position
    public float maxHeightDifference = 2.0f; // Max height difference for spawn position
    public LayerMask terrainLayer; // Layer mask for detecting terrain

    public void InitiateEnemies()
    {
        for (int i = 0; i < levels.Count; i++)
        {
            SpawnEnemies(i);
        }
    }

    public void SpawnEnemies(int levelIndex)
    {
        if (levelIndex < 0 || levelIndex >= levels.Count)
        {
            Debug.LogError("Invalid level index!");
            return;
        }

        Level selectedLevel = levels[levelIndex];

        if (selectedLevel.cowardEnemyPrefab == null || selectedLevel.meleeEnemyPrefab == null)
        {
            Debug.LogError($"Missing enemy prefabs for level: {selectedLevel.levelName}");
            return;
        }

        int cowardEnemiesToSpawn = Mathf.RoundToInt(selectedLevel.totalEnemiesToSpawn * (selectedLevel.cowardEnemyPercentage / 100f));
        int meleeEnemiesToSpawn = selectedLevel.totalEnemiesToSpawn - cowardEnemiesToSpawn;

        // Spawn coward enemies
        for (int i = 0; i < cowardEnemiesToSpawn; i++)
        {
            SpawnEnemy(selectedLevel, selectedLevel.cowardEnemyPrefab);
        }

        // Spawn melee enemies
        for (int i = 0; i < meleeEnemiesToSpawn; i++)
        {
            SpawnEnemy(selectedLevel, selectedLevel.meleeEnemyPrefab);
        }

        Debug.Log($"Enemies spawned for {selectedLevel.levelName}: {cowardEnemiesToSpawn} coward, {meleeEnemiesToSpawn} melee.");
    }

    private void SpawnEnemy(Level level, GameObject enemyPrefab)
    {
        for (int attempt = 0; attempt < 10; attempt++) // Retry up to 10 times to find a valid position
        {
            // Generate a random position within the spawn range
            Vector3 randomPosition = level.levelReference.position + new Vector3(
                Random.Range(-level.spawnRange, level.spawnRange),
                0,
                Random.Range(-level.spawnRange, level.spawnRange)
            );

            Vector3 spawnPosition = GetSpawnPosition(randomPosition);

            // Check if the position is valid
            if (IsPositionOnNavMesh(spawnPosition) &&
                IsPositionReachable(level.levelReference.position, spawnPosition))
            {
                GameObject enemy = Instantiate(enemyPrefab, spawnPosition, Quaternion.identity);
                enemy.SetActive(true);
                enemy.transform.parent = level.levelReference; // Parent to level for organization
                return; // Exit after successful spawn
            }
        }

        Debug.LogWarning($"Failed to find a valid or reachable NavMesh position for an enemy in {level.levelName}.");
    }

    private Vector3 GetSpawnPosition(Vector3 originalPosition)
    {
        RaycastHit hit;
        float terrainHeight = originalPosition.y;

        // Raycast downward to find the terrain surface
        if (Physics.Raycast(originalPosition + Vector3.up * 50, Vector3.down, out hit, 100f, terrainLayer))
        {
            terrainHeight = hit.point.y + spawnHeightOffset; // Adjust for spawn height offset
        }
        else
        {
            Debug.LogWarning("Raycast failed to detect terrain. Using original position.");
        }

        Vector3 adjustedPosition = new Vector3(originalPosition.x, terrainHeight, originalPosition.z);

        // Validate the position on NavMesh
        if (IsPositionOnNavMesh(adjustedPosition))
        {
            return adjustedPosition; // Return valid position
        }

        Debug.LogWarning("Spawn position is not on the NavMesh. Adjusting to fallback height.");
        return originalPosition; // Fallback to original position
    }

    private bool IsPositionOnNavMesh(Vector3 position)
    {
        NavMeshHit hit;
        return NavMesh.SamplePosition(position, out hit, navMeshCheckRadius, NavMesh.AllAreas);
    }

    private bool IsPositionReachable(Vector3 fromPosition, Vector3 toPosition)
    {
        NavMeshPath path = new NavMeshPath();

        // Calculate a path from the reference position to the target position
        if (NavMesh.CalculatePath(fromPosition, toPosition, NavMesh.AllAreas, path))
        {
            // Check if the path is complete and valid
            return path.status == NavMeshPathStatus.PathComplete;
        }

        return false;
    }
}