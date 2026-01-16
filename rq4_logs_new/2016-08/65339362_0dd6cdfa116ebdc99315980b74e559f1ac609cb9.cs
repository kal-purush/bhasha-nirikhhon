using UnityEngine;
using System.Collections;

public class EnemyAI : MonoBehaviour
{
    GameObject[] players;
    GameObject target;
    public float moveSpeed = 4;
    NavMeshAgent agent;
    public float navD;
    public float chaseDist = 50;
    //public float 
    //public bool canSeePlayer;

    // Use this for initialization
    void Start()
    {
        players = GameObject.FindGameObjectsWithTag("Player");
        agent = gameObject.GetComponent<NavMeshAgent>();
        
    }

    // Update is called once per frame
    void Update()
    {
        for(int i = 0; i < players.Length; i++)
        {
            if(i == 0)
            {
                navD = Vector3.Distance(players[i].transform.position, transform.position);
                target = players[i];
            }
            else
            {
                if(Vector3.Distance(players[i].transform.position, transform.position) < navD)
                {
                    navD = Vector3.Distance(players[i].transform.position, transform.position);
                    target = players[i];
                }
            }
        }

        if (navD > chaseDist)
        {
            agent.Stop();
            //agent.speed = 0;
        }
        else if(navD > 2)
        {
            chase();
        }
    }

    /*void OnTriggerEnter(Collider other)
    {
        if (other.gameObject.tag == ("Player"))
        {
           agent.Stop();
        }
    }

    void OnTriggerExit(Collider other)
    {
        if (other.gameObject.tag == ("Player"))
        {
            agent.Resume();
        }
    }*/

    void chase()
    {
        agent.SetDestination(target.transform.position);
        agent.Resume();
    }
}