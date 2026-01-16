using System;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class TheBomb : Enemy
{
    public Animator effects;

    public MuliTimer fuseTimer= new MuliTimer(3f);

    public float expRadius = 0.6f;

    public float boomDist = 0.5f;

    public float knockbackPower = 1;

    public AudioClip ticktock;
    public AudioClip boom;

    // Start is called before the first frame update
    public void Start()
    {
        base.Init();
        setState(State.Moving);
        target = GameObject.FindGameObjectWithTag("Player").transform;
    }

    private void OnDrawGizmos()
    {
        Gizmos.color = Color.yellow;
        Gizmos.DrawWireSphere(transform.position, boomDist);
        Gizmos.color = Color.red;
        Gizmos.DrawWireSphere(transform.position, expRadius);

    }

    // Update is called once per frame
    void Update()
    {
        //move to player
        //if close enough stop moving??????????????? (control by adding removing attacking from movestates)
        //start preparing explosion
        //explode, deal dmg in area, lose hp
        //rest 

        if(state== State.Moving)
        {
            if (Vector3.Distance(transform.position, target.position) < boomDist)
            {
                effects.SetBool("Prep", true);
                setState(State.Attacking);
                getHitSound.Stop();
                getHitSound.PlayOneShot(ticktock);
            }
            return;
        }
        if(state == State.Attacking)
        {
            if (isStunned())
            {
                return;
            }

            if (!fuseTimer.isDone())
            {
                fuseTimer.update(Time.deltaTime);
            }
            else
            {
                fuseTimer.reset();
                effects.SetBool("Prep", false);
                effects.SetTrigger("Exp");
                StartCoroutine(Explode());
                if (health > 0)
                {
                    setState(State.Base);
                }
            }
            return;
        }
    }

    IEnumerator Explode()
    {
        getHitSound.Stop();
        getHitSound.PlayOneShot(boom);

        yield return new WaitForSeconds(0.2f);

        Collider2D[] hitColliders = Physics2D.OverlapCircleAll(transform.position, expRadius);
        //foreach collider in hitColliders
        foreach (Collider2D coll in hitColliders)
        {
            //enemy
            if (coll.gameObject.GetComponent<Enemy>() != null)
            {
                Vector2 direction = transform.position - coll.gameObject.transform.position;

                Vector2 knockback =  (direction.normalized * knockbackPower * -1);

                coll.gameObject.GetComponent<Enemy>().getHit(attackPower, knockback);
            }
            //player
            if (coll.gameObject.GetComponent<RiggedPlayerController>() != null)
            {
                coll.gameObject.GetComponent<RiggedPlayerController>().takeDamage(attackPower, this.gameObject);
            }
        }
        yield return new WaitForSeconds(1.5f);
        setState(State.Moving);

        //this.getHit(1f, Vector2.zero);
    }
}