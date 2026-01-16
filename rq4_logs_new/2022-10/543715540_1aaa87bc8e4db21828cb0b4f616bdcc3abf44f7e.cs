using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.AI;

public class Follower : MonoBehaviour
{
    public Transform player;
    Animator animator;
    NavMeshAgent npc;

    // Start is called before the first frame update
    void Start()
    {
        animator = GetComponent<Animator>();
        npc = GetComponent<NavMeshAgent>();
    }

    // Update is called once per frame
    void Update()
    {
        npc.SetDestination(player.position);
        if (npc.velocity.magnitude > 0.95)
        {
            animator.SetBool("isFollowing", true);
        }
        else
        {
            animator.SetBool("isFollowing", false);
        }
    }
}