using UnityEngine;

public class ArcherEnemy : MonoBehaviour
{
    public float detectionRange = 5f;      
    public float keepDistanceRange = 2f;  
    public float baseMoveSpeed = 2f;      
    public float maxMoveSpeed = 6f;      
    public float shootCooldown = 2f;       
    public GameObject arrowPrefab;         
    public Transform firePoint;            
    public float firePointOffsetX = 0.5f;  

    private Animator animator;
    private Transform player;
    private float nextShootTime;
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

            if (distanceToPlayer <= detectionRange)
            {
                FacePlayer();

                if (distanceToPlayer < keepDistanceRange)
                {
                    KeepDistanceFromPlayer(distanceToPlayer);
                }
                else
                {
                    animator.SetBool("isMoving", false);
                    Shoot();
                }
            }
            else
            {
                animator.SetBool("isShooting", false);
                animator.SetBool("isMoving", false);
            }

            UpdateFirePointPosition();
        }
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

    void KeepDistanceFromPlayer(float distance)
    {
        animator.SetBool("isMoving", true);
        animator.SetBool("isShooting", false); 

        float currentMoveSpeed = Mathf.Lerp(baseMoveSpeed, maxMoveSpeed, 1 - (distance / keepDistanceRange));
        currentMoveSpeed = Mathf.Clamp(currentMoveSpeed, baseMoveSpeed, maxMoveSpeed);

        Vector2 direction = (transform.position - player.position).normalized;
        Vector2 newPosition = (Vector2)transform.position + direction * currentMoveSpeed * Time.deltaTime;
        transform.position = newPosition;
    }

    void UpdateFirePointPosition()
    {
        Vector3 firePointLocalPos = firePoint.localPosition;
        firePointLocalPos.x = facingRight ? firePointOffsetX : -firePointOffsetX;
        firePoint.localPosition = firePointLocalPos;
    }

    void Shoot()
    {
        if (Time.time >= nextShootTime)
        {
            animator.SetBool("isShooting", true);
            nextShootTime = Time.time + shootCooldown;
        }
    }

    void SpawnArrow()
    {
        if (arrowPrefab != null && firePoint != null)
        {
            GameObject arrow = Instantiate(arrowPrefab, firePoint.position, firePoint.rotation);
            Arrow arrowScript = arrow.GetComponent<Arrow>();
            if (arrowScript != null)
            {
                arrowScript.SetDirection(facingRight ? 1 : -1);             }
        }
        else
        {
            Debug.LogError("arrowPrefab hoặc firePoint chưa được gán!");
        }
    }
    void OnDeath()
    {
        animator.SetBool("isShooting", false);
        animator.SetBool("isMoving", false);
        animator.SetBool("isAlive", false);
    }
    void OnDrawGizmosSelected()
    {
        Gizmos.color = Color.red;
        Gizmos.DrawWireSphere(transform.position, detectionRange);
        Gizmos.color = Color.blue;
        Gizmos.DrawWireSphere(transform.position, keepDistanceRange);
    }
}