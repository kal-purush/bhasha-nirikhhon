using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.AI;

public class BasicEnemyController : MonoBehaviour, IEnemy
{
    private NavMeshAgent _agent;
    private Transform _player;
    private EnemyStates _enemyState;
    [SerializeField] private EnemySettings _enemySettings;

    private enum EnemyStates
    {
        Follow,
        Attack
    }

    private void Awake()
    {
        _agent = GetComponent<NavMeshAgent>();
        _player = FindObjectOfType<PlayerController>().transform;
        _agent.SetDestination(_player.position);
    }

    private void Update()
    {
          FollowPlayer();
    }

    public void Initialize()
    {
        _enemyState = EnemyStates.Follow;
    }

    public EnemySettings GetEnemySettings() => _enemySettings;


    public void FollowPlayer()
    {
        transform.LookAt(_player);
        _agent.SetDestination(_player.position);
        if (_agent.remainingDistance <= 1.0f)
        {
            ChangeState(EnemyStates.Attack);
            _agent.speed = 0;
        }
        else
        {
            ChangeState(EnemyStates.Follow);
            _agent.speed = 3;
        }
    }

    private void ChangeState(EnemyStates state)
    {
        if (_enemyState == state) return;
        _enemyState = state;
    }
}