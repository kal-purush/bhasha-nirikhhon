using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class SpawnManager : MonoBehaviour
{
    [SerializeField]
    private GameObject enemySpawnPrefab;
    [SerializeField]
    private GameObject[] powerupSpawnsPrefabs;

    // Start is called before the first frame update
    void Start()
    {
        StartCoroutine(SpawnEnemyRoutine());
        StartCoroutine(SpawnPowerupsRoutine());
    }

    private IEnumerator SpawnEnemyRoutine()
    {
        while (true)
        {
            Instantiate(enemySpawnPrefab, new Vector3(Random.Range(-8.0f, 8.0f), 8.0f, 0), Quaternion.identity);
            yield return new WaitForSeconds(3.0f);
        }
    }

    private IEnumerator SpawnPowerupsRoutine()
    {
        while (true)
        {
            int powerupsRandom = Random.Range(0, 3);
            Instantiate(powerupSpawnsPrefabs[powerupsRandom], new Vector3(Random.Range(-8.0f, 8.0f), 8.0f, 0), Quaternion.identity);
            yield return new WaitForSeconds(4.0f);
        }
    }
}