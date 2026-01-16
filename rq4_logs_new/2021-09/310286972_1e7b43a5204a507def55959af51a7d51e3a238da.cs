using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.AI;

public class FlyingEnemy_Y : MonoBehaviour
{
    public GameObject parachute;
    private bool isFlying = true;
    public Enemy_Y enemyScr;
    private Rigidbody rb;
    private NavMeshAgent navMeshAgent;
    private Animator animator;
    private float rayCycle = 1f;
    private float rayTimer;

    private void Start()
    {
        enemyScr = GetComponent<Enemy_Y>();
        rb = GetComponent<Rigidbody>();
        navMeshAgent = GetComponent<NavMeshAgent>();
        animator = GetComponent<Animator>();

        enemyScr.enabled = false;
        navMeshAgent.enabled = false;
        animator.enabled = false;
        rb.isKinematic = false;

        Invoke("NotIsTrigger", 2f);
    }

    private void Update()
    {
        if (isFlying)
        {
            rayTimer += Time.deltaTime;
            if (rayTimer >= rayCycle)
            {
                rayTimer = 0f;
                RaycastHit hit;
                if (Physics.Raycast(transform.position, -transform.up, out hit, 0.1f))
                {
                    enemyScr.enabled = true;
                    navMeshAgent.enabled = true;
                    animator.enabled = true;
                    rb.isKinematic = true;
                }
                Debug.DrawRay(transform.position, -transform.up, Color.red, 1f);
            }
        }
    }

    private void NotIsTrigger()
    {
        GetComponent<Collider>().isTrigger = false;
    }
}