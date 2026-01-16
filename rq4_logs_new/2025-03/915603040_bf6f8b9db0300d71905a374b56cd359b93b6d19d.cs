using System;
using UnityEngine;

public class ArcherNMelleEnemy : MonoBehaviour
{
    public float detectionRange = 5f;
    public float meleeRange = 1.5f;
    public float shootCooldown = 2f;
    public float meleeCooldown = 1f;
    public GameObject arrowPrefab;
    public Transform firePoint;
    public float firePointOffsetX = 0.5f;

    private Animator animator;
    private Transform player;
    private float nextAttackTime;
    private bool facingRight = true;
    private Damageable damageable;

    void Start()
    {
        animator = GetComponent<Animator>();
        player = GameObject.FindGameObjectWithTag("Player").transform;
        damageable = GetComponent<Damageable>();
        damageable.damageableDeath.AddListener(OnDeath);
    }

    void Update()
    {
        if (damageable.IsAlive)
        {
            float distanceToPlayer = Vector2.Distance(transform.position, player.position);
            FacePlayer();

            if (Time.time >= nextAttackTime) 
            {
                if (distanceToPlayer <= meleeRange)
                {
                    MeleeAttack(); 
                }
                else if (distanceToPlayer <= detectionRange)
                {
                    Shoot(); 
                }
                else
                {
                    animator.SetBool("isShooting", false);
                    animator.SetBool("isAttacking", false);
                }
            }
        }

        UpdateFirePointPosition();
    }


    void FacePlayer()
    {
        if (player.position.x < transform.position.x)
        {
            transform.localScale = new Vector3(-1, 1, 1);
            facingRight = false;
        }
        else
        {
            transform.localScale = new Vector3(1, 1, 1);
            facingRight = true;
        }
    }

    void UpdateFirePointPosition()
    {
        Vector3 firePointLocalPos = firePoint.localPosition;
        firePointLocalPos.x = facingRight ? firePointOffsetX : -firePointOffsetX;
        firePoint.localPosition = firePointLocalPos;
    }

    void Shoot()
    {
        if (Time.time >= nextAttackTime)
        {
            animator.SetBool("isShooting", true);
            animator.SetTrigger("shootTrigger");
            nextAttackTime = Time.time + shootCooldown;
        }
    }
    private void MeleeAttack()
    {
        animator.SetBool("isAttacking", true);
        animator.SetTrigger("meleeTrigger");
        nextAttackTime = Time.time + meleeCooldown;

        Invoke(nameof(ResetMeleeAttack), meleeCooldown - 0.1f);
    }

    void ResetMeleeAttack()
    {
        animator.SetBool("isAttacking", false);
    }

    void SpawnArrow()
    {
        if (arrowPrefab != null && firePoint != null)
        {
            GameObject arrow = Instantiate(arrowPrefab, firePoint.position, firePoint.rotation);
            EnemyArrow arrowScript = arrow.GetComponent<EnemyArrow>();
            if (arrowScript != null)
            {
                arrowScript.SetDirection(facingRight ? 1 : -1);
            }
            arrow.transform.localScale = arrowPrefab.transform.localScale;
        }
        else
        {
            Debug.LogError("arrowPrefab hoặc firePoint chưa được gán!");
        }
    }

    void OnDeath()
    {
        animator.SetTrigger("dieTrigger");
        animator.SetBool("isShooting", false);
        animator.SetBool("isAttacking", false);
    }



    void OnDrawGizmosSelected()
    {
        Gizmos.color = Color.red;
        Gizmos.DrawWireSphere(transform.position, detectionRange);
        Gizmos.color = Color.blue;
        Gizmos.DrawWireSphere(transform.position, meleeRange);
    }
}