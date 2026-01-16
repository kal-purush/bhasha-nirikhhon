using UnityEngine;
using System.Collections;

public class AI_movement : MonoBehaviour
{

    public Transform[] patrolWayPoints;

    private Transform player;
    private int Target = 0;
    private Animator anim;

    private NavMeshAgent agent;
    private bool isPlayerSeen = false;

    void Start()
    {
        agent = GetComponent<NavMeshAgent>();

        agent.autoBraking = false;
        Patrol();
        anim = GetComponent<Animator>();

        player = GameObject.FindGameObjectWithTag("Player").transform;


    }

    void Update()
    {
        if (agent.remainingDistance < 0.5f)
            Patrol();

        if (isPlayerSeen)
        {
            transform.LookAt(player);


            Debug.Log("Following player");
            if (Vector3.Distance(transform.position, player.position) < 2.25f)
            {
                Debug.Log("Caught player");
                anim.SetBool("isPlayerInRange", true);
                transform.localRotation = Quaternion.Euler(0.0f, transform.eulerAngles.y, 0.0f);
                agent.speed = 0;         
            }
            else
            {
                transform.position += transform.forward * 5 * Time.deltaTime;
            }


        }
    }

    void OnTriggerEnter(Collider other)
    {
        transform.LookAt(player);
        isPlayerSeen = true;
        anim.SetBool("isPlayerSeen", true);
        agent.speed = 0;
        Debug.Log("Player Entry Detected");

    }
    void OnTriggerExit(Collider other)
    {
        anim.SetBool("isPlayerSeen", false);

    }


    void Patrol()
    {
        if (Target == patrolWayPoints.Length)
        {
            Target = 0;
        }

        agent.destination = patrolWayPoints[Target].transform.position;
        Target++;
    }

}