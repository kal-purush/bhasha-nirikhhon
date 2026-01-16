using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.AI;

public class KingAI : MonoBehaviour
{
    public Transform player; 
    private NavMeshAgent agent;
    private Animator anim;

    void Start()
    {
        agent = GetComponent<NavMeshAgent>();

        anim = GetComponent<Animator>();
        anim.SetInteger("WeaponType_int", 0);

        player = GameObject.FindGameObjectWithTag("Player").transform;

        agent.SetDestination(player.position);
        agent.stoppingDistance = 5f;
    }

    void Update()
    {
        FacePlayer();

        float distanceToPlayer = Vector3.Distance(player.position, transform.position); ;

        if (distanceToPlayer > 5f)
        {
            agent.speed = 3.5f;
            agent.SetDestination(player.position);
            anim.SetFloat("Speed_f", 0.7f);
            anim.SetInteger("Animation_int", 0);
        } else
        {
            anim.SetFloat("Speed_f", 0f);
            anim.SetInteger("Animation_int", 1);
        }
    }

    void FacePlayer()
    {
        Vector3 directionToTarget = (player.position - transform.position).normalized;
        directionToTarget.y = 0;
        Quaternion lookRotation = Quaternion.LookRotation(directionToTarget);
        transform.rotation = Quaternion.Slerp(transform.rotation, lookRotation, 10 * Time.deltaTime);
    }
}