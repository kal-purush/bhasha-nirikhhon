using System.Collections;
using System.Collections.Generic;
using UnityEditor;
using UnityEngine;

public class Scorpion : Enemy
{
    public GameObject roamTarget;
    public Transform playerTransform;

    public float maxiDistToPlayer = 5f;
    public float minDistToPlayer = 1.75f;
    public float attackDist = 0.75f;

    public float chargeSpeed = 7.5f;

    public float beforeAttackWaitTime = 1f;
    public float beforeAttackWaitTimer = 0;

    public float afterAttackWaitTime = 2f;
    public float afterAttackWaitTimer = 0;

    public bool hasAttacked = false;


    // Start is called before the first frame update
    void Start()
    {
        base.Init();
        setState(State.Idle);
        playerTransform = GameObject.FindGameObjectWithTag("Player").transform;
        roamTarget.transform.position = Vector3.zero;
        target = roamTarget.transform;
    }

    private void OnDrawGizmos()
    {
        Gizmos.color = Color.green;
        Gizmos.DrawWireSphere(transform.position, minDistToPlayer);
        Gizmos.color = Color.red;
        Gizmos.DrawWireSphere(transform.position, maxiDistToPlayer);
        Gizmos.color = Color.blue;
        Gizmos.DrawWireSphere(transform.position, attackDist);
    }

    // Update is called once per frame
    void Update()
    {
        //IDLE- idle patrols - aggros player within zone
        //MOVING -if aggrod player moves to player's orbit
        //PREP - circles player, when ready saves player loc and attacks in dir
        //Attacking - extends sting to saved player loc
        //Post attack - rest at loc for few secs
        //back to Idle after rest 



        if (state == State.Idle)
        {
            if (roamTarget.transform.position == Vector3.zero)
            {
                Vector3 goal = GameManager.Instance.getSpawnPoint(transform.position, 2, 7);
                roamTarget = Instantiate(roamTarget, goal, Quaternion.identity);
                target = roamTarget.transform;
                return;
            }
            if (Vector3.Distance(transform.position, playerTransform.position) > maxiDistToPlayer)
            {
                Vector3 goal = GameManager.Instance.getSpawnPoint(playerTransform.position, 0, 4);
                roamTarget.transform.position = goal;
                return;
            }
            if (Vector3.Distance(transform.position, target.position) < 0.5f)
            {
                Vector3 goal = GameManager.Instance.getSpawnPoint(transform.position, 2, 7);
                roamTarget.transform.position = goal;
                return;
            }
            if (Vector3.Distance(transform.position, playerTransform.position) < minDistToPlayer)
            {
                target = playerTransform;
                setState(State.Moving);
                return;
            }

        }
        if(state == State.Moving)
        {
            if(target!=playerTransform)
            {
                target = playerTransform;
            }
            if((Vector3.Distance(transform.position, target.position) < attackDist)){
                setState(State.Preparing);
                beforeAttackWaitTimer= 0;
                afterAttackWaitTimer = 0;
                roamTarget.transform.position = Grid.adjustWoldPosToNearestCell(playerTransform.position, GameManager.Instance.gridCellSize);
                target = roamTarget.transform;
                return;
            }
        }
        if(state == State.Preparing)
        {
            spriteRenderer.flipX = false;
            if (beforeAttackWaitTimer < beforeAttackWaitTime)
            {
                beforeAttackWaitTimer += Time.deltaTime;
                //move to player
                roatateTo(target.position);

            }
            else
            {
                beforeAttackWaitTimer = 0f;
                setState(State.Attacking);
            }
        }
        if(state == State.Attacking)
        {
            if (hasAttacked)
            {
                if (afterAttackWaitTimer < afterAttackWaitTime)
                {
                    afterAttackWaitTimer += Time.deltaTime;
                }
                else
                {
                    afterAttackWaitTimer = 0f;
                    hasAttacked = false;
                    rb.mass = 1;
                    setState(State.Idle);
                }
            }
            else
            {
                if ((Vector3.Distance(transform.position, target.position) > 0.1f))
                {
                    Vector3 directionToTarget = (target.position - transform.position).normalized;
                    moveInDirectionWithSpeedModifier(directionToTarget, chargeSpeed);
                }
                else
                {
                    transform.rotation = Quaternion.Euler(new Vector3(0, 0, 0));
                    hasAttacked= true;
                    rb.mass = 500;
                    rb.velocity = Vector3.zero;

                }
            }

        }
    }

    public override void Death()
    {
        Destroy(roamTarget);
        base.Death();
    }

    private void OnCollisionEnter2D(Collision2D collision)
    {
        if (state == State.Attacking && !hasAttacked && (collision.gameObject.tag == "Enemy" || collision.gameObject.tag == "Wall") )
        {
            transform.rotation = Quaternion.Euler(new Vector3(0, 0, 0));
            rb.mass = 1;
            roamTarget.transform.position = Vector3.zero;
            setState(State.Idle);
        }
    }

    public void roatateTo(Vector3 targ)
    {
        targ.x = targ.x - transform.position.x;
        targ.y = targ.y - transform.position.y;
        //if (targ.x < 0)
        //{
        //    targ.x *= -1;
        //    spriteRenderer.flipX = true;
        //}
        //if (targ.y < 0)
        //{
        //    targ.y *= -1;
        //}

        float angle = Mathf.Atan2(targ.y, targ.x) * Mathf.Rad2Deg;
        transform.rotation = Quaternion.Euler(new Vector3(0, 0, angle));
    }
}