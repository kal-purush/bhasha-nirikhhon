using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.AI;

public abstract class BaseEnemy : MonoBehaviour {

    public float Health = 250f;
    public float Armor = 10f;
    public float Mana = 100f;
    public float Strength = 5f;
    public float Dexterity = 5f;
    public float Intelligence = 5f;
    public float Endurance = 5f;
    public float aggroRange = 5f;

    Transform target;
    NavMeshAgent agent;   

    void Start()
    {
        target = PlayerManager.instance.player.transform;
        agent = GetComponent<NavMeshAgent>();
        Debug.Log("Spieler gefunden");
    }
    void Update()
    {
        float distance = Vector3.Distance(target.position, transform.position);

        if (distance <= aggroRange)
        {            
            agent.SetDestination(target.position);
            Debug.Log("Angriff!");

            if (distance <= agent.stoppingDistance)
            {
                FaceTarget();

                //Attack Target

            }
        }
    }

    void FaceTarget()
    {
        Vector3 direction = (target.position - transform.position).normalized;
        Quaternion lookRotation = Quaternion.LookRotation(new Vector3(direction.x, 0, direction.z));
        transform.rotation = Quaternion.Slerp(transform.rotation, lookRotation, Time.deltaTime * 5f);
    }



    public void Attack()
    {
        //Do Damage to attacked person
    }

    public void Walk()
    {

    }

    private void OnDrawGizmosSelected()
    {
        Gizmos.color = Color.red;
        Gizmos.DrawWireSphere(transform.position, aggroRange);
    }
}