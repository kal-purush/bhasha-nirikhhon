using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class DropEnemies : MonoBehaviour
{
    // Pooling
    [SerializeField] private Transform enemy = default;
    [SerializeField] private int numOfEnemies;
    [SerializeField] private float enemyDropTime;
    
    private float enemyDropTimer;
    private int enemiesLeft;
    private TargetingAI targetingAI;
    private bool activated = false;
    private void Awake()
    {
        targetingAI = GetComponent<TargetingAI>();
        enemiesLeft = numOfEnemies;
    }

    // Update is called once per frame
    void Update()
    {
        activated = true;
        if (targetingAI.CheckNoTarget())
        {
            enemyDropTimer = enemyDropTime;
        }
        if (enemyDropTimer <= 0)
        {
            SpawnEnemy();
            enemiesLeft--;
            enemyDropTimer = enemyDropTime;
            return;
        }
        else
        {
            enemyDropTimer -= Time.deltaTime;
        }
        if(enemiesLeft <= 0)
        {
            GetComponent<EnemyHealth>().UpdateCurrentHealth(-100);
            SpawnEnemy();
            SpawnEnemy();
        }
        else if(GetComponent<EnemyHealth>().GetHealthPercentage() < 50 && enemiesLeft > 0)
        {
            GetComponent<EnemyHealth>().UpdateCurrentHealth(100);
            SpawnEnemy();
            enemiesLeft--;
            enemyDropTimer = enemyDropTime;
        }
    }
    
    private void SpawnEnemy()
    {
        float randomX = Random.Range(-0.5f, 0.6f);
        float randomY = Random.Range(-0.5f, 0.6f);

        Instantiate(enemy, new Vector3 (transform.position.x + randomX, transform.position.y + randomY, transform.position.z), Quaternion.identity);
    }
    private void OnDisable()
    {
        if (!activated)
        {
            return;
        }
        for (int i = enemiesLeft; i > 0; i--)
        {
            SpawnEnemy();
        }
    }
}