using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Ranged : Enemy
{
    [Header("Shooting")]
    public Projectile projectile;
    public MuliTimer attackPrepTime = new MuliTimer(0.5f);
    public float projectileCount = 3f;


    [Header("Targeting")]

    public GameObject roamTargePrefab;
    public GameObject roamTarget;
    public Transform playerTransform;


    [Header("Distance")]

    public float shootingPosDist = 2f;

    public float maxiDistToPlayer = 5f;
    public float minDistToPlayer = 1.75f;
    public float attackDist = 0.75f;

    // Start is called before the first frame update
    void Start()
    {
        base.Init();
        setState(State.Idle);
        playerTransform = GameObject.FindGameObjectWithTag("Player").transform;
    }

    public override void Death()
    {
        Destroy(roamTarget);
        base.Death();
    }

    // Update is called once per frame
    void Update()
    {

        //find a spot X dist from player
        //is spot is far, find new spot
        //if at spot -> fire projectile at player loc
        //wait some time
        //move closer and repeat
        //if out of bullets -> charge

        if (state == State.Idle)
        {
            if (roamTarget == null )
            {
                //get direction to player
                //find spot x distance from player in that direction
                //spawn goal around that spot
                //if not possible - just spawn around the player

                Vector3 directionToPlayer = (transform.position -playerTransform.position).normalized;
                Vector3 shootingPos = playerTransform.position + directionToPlayer* shootingPosDist;
                Vector3 goal = GameManager.Instance.getSpawnPoint(shootingPos, 0, 2);

                roamTarget = Instantiate(roamTargePrefab, goal, Quaternion.identity);
                target = roamTarget.transform;
                return;
            }
            if (Vector3.Distance(roamTarget.transform.position, playerTransform.position) > maxiDistToPlayer)
            {
                Vector3 directionToPlayer = (transform.position - playerTransform.position).normalized;
                Vector3 shootingPos = playerTransform.position + directionToPlayer * shootingPosDist;
                Vector3 goal = GameManager.Instance.getSpawnPoint(shootingPos, 0, 2);

                roamTarget.transform.position = goal;
                return;
            }
            if (Vector3.Distance(transform.position, playerTransform.position) < Vector3.Distance(transform.position, roamTarget.transform.position))
            {
                setState(State.Attacking);
                return;
            }
            if (Vector3.Distance(transform.position, roamTarget.transform.position) < 0.2f)
            {
                setState(State.Attacking);
                return;
            }
        }
        if (state == State.Attacking)
        {

            if (!attackPrepTime.isDone())
            {
                attackPrepTime.update(Time.deltaTime);
                return;
            }
            else
            {
                attackPrepTime.reset();
                Vector3 directionToPlayer = (playerTransform.position - transform.position).normalized;
                Projectile spawenProj = Instantiate(projectile, transform.position, Quaternion.identity);
                spawenProj.Setup(directionToPlayer, attackPower);
                projectileCount -= 1f;
                setState(State.Preparing); 
                return;
            }

            //decrement bullets
            //wait a bit
        }
        if (state == State.Preparing)
        {
            //wait a bit
            //if player is too fat - zero out roamtarget and set to idle
            //if out of bullets set to move
            if (!attackPrepTime.isDone())
            {
                attackPrepTime.update(Time.deltaTime);
                return;
            }
            else
            {
                attackPrepTime.reset();

                if (projectileCount <= 0)
                {
                    movemetSpeed *= 2f;
                    Destroy(roamTarget);
                    roamTarget = null;
                    setState(State.Moving);
                    target = playerTransform;

                }
                else
                {
                    Destroy(roamTarget);
                    roamTarget = null;
                    setState(State.Idle);
                }
            }
        }
        if (state == State.Moving)
        {
            //move to player
        }

    }

}
    