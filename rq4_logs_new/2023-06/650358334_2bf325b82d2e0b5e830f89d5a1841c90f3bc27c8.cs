using UnityEngine;
using UnityEngine.AI;

public class EnemyAIStateMachine : MonoBehaviour
{
    public enum State
    {
        Idle,
        Chasing,
        Attacking
    }

    public State currentState;
    public Transform player;
    private NavMeshAgent agent;
    public float stoppingDistance = 2f;
    public float detectionRange = 10f;

    private void Start()
    {
        agent = GetComponent<NavMeshAgent>();
        agent.stoppingDistance = stoppingDistance;
        currentState = State.Idle;
    }

    private void Update()
    {
        switch (currentState)
        {
            case State.Idle:
                if (Vector3.Distance(transform.position, player.position) <= detectionRange)
                {
                    currentState = State.Chasing;
                }
                break;

            case State.Chasing:
                ChasePlayer();
                break;

            case State.Attacking:
                AttackPlayer();
                break;
        }
    }

    private void ChasePlayer()
    {
        if (agent.enabled)
        {
            agent.SetDestination(player.position);
            if (Vector3.Distance(transform.position, player.position) <= stoppingDistance)
            {
                currentState = State.Attacking;
            }
        }
    }

    private void AttackPlayer()
    {
        if (agent.enabled)
        {
            agent.isStopped = true;
        }
    }
}