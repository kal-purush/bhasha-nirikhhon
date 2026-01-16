using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class EnemySpawner : MonoBehaviour
{
    public GameObject enemyPrefab;
    public float xMin = -25;
    public float xMax = 25;

    public float zMin = -25;
    public float zMax = 25;

    public float numEnemies = 10;
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    public void Spawn() {
        for (int i = 0; i < numEnemies; i++)
        {
            Vector3 enemyPosition = new Vector3(transform.position.x, 0, transform.position.z);
            enemyPosition.x += Random.Range(xMin, xMax);
            enemyPosition.z += Random.Range(zMin, zMax);
            // Random rotation
            Quaternion enemyRotation = Quaternion.Euler(0, Random.Range(0, 360), 0);
            GameObject spawnedEnemy = Instantiate(enemyPrefab, enemyPosition, enemyRotation) as GameObject;

            spawnedEnemy.transform.parent = gameObject.transform;
        }
    }
}