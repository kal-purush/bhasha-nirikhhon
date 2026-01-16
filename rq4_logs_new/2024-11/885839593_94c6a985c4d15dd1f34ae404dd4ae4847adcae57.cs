using UnityEngine;
using UnityEngine.AI;

public class Boss : MonoBehaviour, PlayerInterface
{
    public float health = 100f;
    public float damage = 20f;
    public float attackRange = 10f;
    public float safeDistance = 5f;
    public float fireRate = 1f;
    public float bulletSpeed = 20f;
    public float accuracy = 0.5f;
    public GameObject bulletPrefab;
    public GameObject specialBulletPrefab; 
    public Transform firePoint;
    public Transform firePointLeft; 
    public Transform firePointRight;
    public Transform player;

    private NavMeshAgent navMeshAgent;
    private float nextFireTime = 0f;
    private bool isPlayerInRange = false;
    private bool isAttacking = false;
    bool isDead;
    Animator anim;

    // Biến để theo dõi lượng máu đã mất
    public float damageTaken = 0f;

    int LayerAttack, LayerSKill1;

    void Start()
    {
        anim = GetComponent<Animator>();
        navMeshAgent = GetComponent<NavMeshAgent>();
        player = GameObject.FindWithTag("Player").transform;
        LayerAttack = anim.GetLayerIndex("Attack");
        LayerSKill1 = anim.GetLayerIndex("Skill1");

        InvokeRepeating("CheckHealthAndUpdate", 0f, 1f);
    }

    void Update()
    {
        Die();

        if (isDead)
        {
            return;
        }

        Idle();
        float distanceToPlayer = Vector3.Distance(transform.position, player.position);

        isPlayerInRange = distanceToPlayer <= attackRange;

        if (isPlayerInRange)
        {
            ChasePlayer();
        }
        else
        {
            navMeshAgent.SetDestination(transform.position);
        }

        if (damageTaken >= 20f)
        {
            anim.SetTrigger("Skill1");
            damageTaken = 0f; 
            anim.SetLayerWeight(LayerAttack, 0);
            anim.SetLayerWeight(LayerSKill1, 1);
            Invoke("TurnOffSkill", 2f);
        }

        if (isPlayerInRange && Time.time >= nextFireTime && !isAttacking && anim.GetCurrentAnimatorStateInfo(LayerAttack).normalizedTime >= 1)
        {
            isAttacking = true;
            anim.SetTrigger("Attack");
        }

        RotateTowardsPlayer();
    }

    void TurnOffSkill()
    {
        anim.SetLayerWeight(LayerAttack, 1);
        anim.SetLayerWeight(LayerSKill1, 0);
        isAttacking = false; 
    }


    void ChasePlayer()
    {
        float distanceToPlayer = Vector3.Distance(transform.position, player.position);

        if (distanceToPlayer > safeDistance)
        {
            navMeshAgent.SetDestination(player.position);
        }
        else
        {
            navMeshAgent.SetDestination(transform.position);
        }
    }
    public void Shoot()
    {
        TakeDamage(5);
    }
    public void TakeDamage(float amount)
    {
        health -= amount;
        damageTaken += amount;
        anim.SetTrigger("Hurt2");
        if (health <= 0)
        {
            isDead = true;
        }
    }

    void Idle()
    {
        anim.SetBool("Idle", isPlayerInRange);
    }

    void Die()
    {
        anim.SetBool("Death", isDead);
    }


    public void Fire()
    {
        nextFireTime = Time.time + 1f / fireRate;
        isAttacking = false;

        Vector3 directionToPlayer = (player.position - firePoint.position).normalized;

        float deviationAmount = 1.0f - accuracy;

        Vector3 deviationVector = new Vector3(
            Random.Range(-deviationAmount, deviationAmount),
            Random.Range(-deviationAmount / 2, deviationAmount / 2),
            0
        );

        Vector3 firingDirection = (directionToPlayer + deviationVector).normalized;

        GameObject bullet = Instantiate(bulletPrefab, firePoint.position, firePoint.rotation);
        Rigidbody bulletRb = bullet.GetComponent<Rigidbody>();
        bulletRb.velocity = firingDirection * bulletSpeed;
    }

    public void FireSpecialBullet()
    {
        nextFireTime = Time.time + 1f / fireRate;

        FireBulletFrom(firePointLeft);

        FireBulletFrom(firePointRight);
    }

    void FireBulletFrom(Transform firePoint)
    {
        Vector3 directionToPlayer = (player.position - firePoint.position).normalized;

        for (int i = -1; i <= 1; i++)
        {
            Vector3 firingDirection = directionToPlayer + firePoint.right * i * 0.5f;
            firingDirection.Normalize();

            GameObject bullet = Instantiate(specialBulletPrefab, firePoint.position, firePoint.rotation);
            Rigidbody bulletRb = bullet.GetComponent<Rigidbody>();
            bulletRb.velocity = firingDirection * bulletSpeed;
        }
    }

    void RotateTowardsPlayer()
    {
        Vector3 directionToPlayer = player.position - transform.position;
        directionToPlayer.y = 0;

        Quaternion rotation = Quaternion.LookRotation(directionToPlayer);
        transform.rotation = Quaternion.Slerp(transform.rotation, rotation, Time.deltaTime * 5f);
    }
}