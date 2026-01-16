using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.AI;

public class EnemyEnable : MonoBehaviour
{
    [SerializeField] Enemy enemy;
    [SerializeField] Status status;
    [SerializeField] EnemyArtsInstant enemyArtsInstant;
    [SerializeField] NavMeshAgent nav;

    public void OnEnemyEnable(bool isActive)
    {
        enemy.enabled = isActive;
        status.enabled = isActive;
        enemyArtsInstant.enabled = isActive;
        nav.isStopped = isActive ? false : true;
    }
}