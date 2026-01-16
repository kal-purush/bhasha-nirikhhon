using System.Collections;
using UnityEngine;

public class SpawnerController : MonoBehaviour
{
    [SerializeField] private GameObject _bomberPrefab; // The prefab to spawn
    [SerializeField] private float _spawnInterval = 1f; // The interval between spawns
    [SerializeField] private int _waveCount = 3; // The number of waves to spawn
    [SerializeField] private float _waveInterval = 5f; // The interval between waves

    [SerializeField] private int _currentWave = 0; // The current wave number

    private void Start()
    {
        // Start spawning waves
        StartCoroutine(SpawnWaves());
    }

    private IEnumerator SpawnWaves()
    {
        while (_currentWave < _waveCount)
        {
            // Spawn a wave
            SpawnWave();

            // Wait for the wave interval
            yield return new WaitForSeconds(_waveInterval);
        }
    }

    private void SpawnWave()
    {
        // Spawn enemies or objects in the wave
        // You can customize the spawn positions, rotations, and other configurations here

        // Example: Spawn 5 enemies in a line
        for (int i = 0; i < 5; i++)
        {
            Vector3 spawnPosition = new Vector3(transform.position.x, transform.position.y + 0.5f, 0f);
            Quaternion spawnRotation = Quaternion.identity;
            Instantiate(_bomberPrefab, spawnPosition, spawnRotation);
        }

        // Increase the wave number
        _currentWave++;
    }
}