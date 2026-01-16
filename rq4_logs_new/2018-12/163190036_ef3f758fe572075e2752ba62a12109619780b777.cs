using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.AI;

public class PigScript : MonoBehaviour
{

    NavMeshAgent pigAgent;
    [SerializeField] GameObject player;

    [SerializeField] float distanceCharge;
    Animator pigAnimator;
    Rigidbody pigBody;
    bool isCharging;

    float chargeTime;

    bool isMoving;
    // Update is called once per frame

    void Start()
    {
        pigAnimator = GetComponent<Animator>();
        pigAgent = GetComponent<NavMeshAgent>();
        pigBody = GetComponent<Rigidbody>();
        chargeTime = 0f;
        // pigAnimator.SetBool("New State 0", true);

    }

    void Update()
    {
        if (Time.timeSinceLevelLoad - chargeTime > 2)
        {
            pigBody.velocity = new Vector3(0, 0, 0);
            transform.LookAt(player.transform.position);
            if (Vector3.Distance(player.transform.position, transform.position) > distanceCharge && !isCharging)
            {
                FollowMage();
                Debug.Log("distance");
            }
            else if (!isCharging)
            {
                pigAgent.speed = 0.0f;
                pigAnimator.SetFloat("a", 3f);
                isCharging = true;
                transform.LookAt(player.transform.position);
                Invoke("Charging", 2);
                Debug.Log("Charging");
            }
        }
        else
        {
            FollowMage();
            Debug.Log("Waiting for cooldown");
        }
    }

    void FollowMage()
    {
        pigAnimator.SetFloat("a", 1.5f);
        pigAgent.speed = 3.5f;
        pigAgent.SetDestination(new Vector3(player.transform.position.x, 0.5f, player.transform.position.z));
    }

    void Charging()
    {
        pigBody.velocity = transform.forward * 50;
        pigAnimator.SetFloat("a", 1.5f);
        isCharging = false;
        chargeTime = Time.timeSinceLevelLoad;
    }

    void OnCollisionEnter(Collision other)
    {
        // if(other.gameObject.tag == )
        pigBody.velocity = new Vector3(0, 0, 0);
        transform.LookAt(player.transform.position);
        // isCharging = false;
    }
}