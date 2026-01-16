using System.Collections;
using UnityEngine;

public class MeowGoro : MonoBehaviour
{
    public float walkAcceleration = 3f; // Không còn cần thiết nhưng giữ lại để tương thích
    public float maxSpeed = 3f;         // Không còn cần thiết nhưng giữ lại để tương thích
    public float walkStopRate = 0.05f;
    public DetectionZone attackZone;
    public GameObject healthPickupPrefab;
    public Transform target;
    public float chaseSpeed = 4f;
    public float attackRange = 1.5f;
    public bool isKnockedBack = false;
    public float knockbackTime = 0.2f;
    public bool isChasing = false;
    private Vector2 targetPosition;

    public GameObject arrowPrefab; // Prefab mũi tên
    public Transform shootPoint;

    Rigidbody2D rb;
    TouchingDirections touchingDirections;
    protected Animator animator;
    Damageable damageable;
    private bool isAttacking = false;

    public AudioSource audioSource;
    public AudioClip attackSound;

    public enum WalkableDirection
    {
        Right, Left
    }
    private WalkableDirection _walkDirection;
    private Vector2 walkDirectionVector = Vector2.right;

    public WalkableDirection WalkDirection
    {
        get { return _walkDirection; }
        set
        {
            if (_walkDirection != value)
            {
                gameObject.transform.localScale = new Vector2(gameObject.transform.localScale.x * -1, gameObject.transform.localScale.y);
                if (value == WalkableDirection.Right)
                {
                    walkDirectionVector = Vector2.right;
                }
                else if (value == WalkableDirection.Left)
                {
                    walkDirectionVector = Vector2.left;
                }
            }
            _walkDirection = value;
        }
    }

    public bool _hasTarget = false;
    public bool HasTarget
    {
        get { return _hasTarget; }
        private set
        {
            _hasTarget = value;
            animator.SetBool(AnimationStrings.hasTarget, value);
        }
    }

    public bool CanMove
    {
        get
        {
            return animator.GetBool(AnimationStrings.canMove);
        }
    }

    public float AttackCooldown
    {
        get
        {
            return animator.GetFloat(AnimationStrings.attackCooldown);
        }
        private set
        {
            animator.SetFloat(AnimationStrings.attackCooldown, Mathf.Max(value, 0));
        }
    }
    public void SpawnArrow()
    {
        GameObject arrow = Instantiate(arrowPrefab, shootPoint.position, Quaternion.identity);
        EnemyArrow arrowScript = arrow.GetComponent<EnemyArrow>();

        int direction = (WalkDirection == WalkableDirection.Right) ? 1 : -1;
        arrowScript.SetDirection(direction);

    }
    void Awake()
    {
        rb = GetComponent<Rigidbody2D>();
        touchingDirections = GetComponent<TouchingDirections>();
        animator = GetComponent<Animator>();
        damageable = GetComponent<Damageable>();
        WalkDirection = WalkableDirection.Left;
        audioSource = GetComponent<AudioSource>();
        Collider2D enemyCollider = GetComponent<Collider2D>();
        Collider2D playerCollider = GameObject.FindGameObjectWithTag("Player")?.GetComponent<Collider2D>();
        if (playerCollider != null)
        {
            Physics2D.IgnoreCollision(enemyCollider, playerCollider);
        }
    }
    public void PlayAttackSound()
    {
        if (attackSound != null && audioSource != null)
        {
            audioSource.PlayOneShot(attackSound);
        }
    }

    protected virtual void Update()
    {
        HasTarget = attackZone.detectedColliders.Count > 0;

        // Giảm AttackCooldown theo thời gian
        if (AttackCooldown > 0)
        {
            AttackCooldown -= Time.deltaTime;
        }

        if (HasTarget) // Nếu phát hiện Player
        {
            target = attackZone.detectedColliders[0].transform; // Lấy vị trí Player
            float distanceToPlayer = Vector2.Distance(transform.position, target.position);

            if (distanceToPlayer > attackRange)
            {
                isChasing = true; // Bắt đầu đuổi
            }
            else
            {
                isChasing = false;
                if (!isAttacking && AttackCooldown <= 0) // Chỉ tấn công khi không đang tấn công và cooldown hết
                {
                    PerformRandomAttack();
                }
            }
        }
        else
        {
            target = null;
            isChasing = false;
        }
    }

    protected virtual void FixedUpdate()
    {
        if (isKnockedBack)
        {
            return;
        }

        if (target != null && isChasing && CanMove) // Chỉ di chuyển khi có mục tiêu và đang đuổi
        {
            float direction = Mathf.Sign(target.position.x - transform.position.x); // Xác định hướng
            rb.linearVelocity = new Vector2(direction * chaseSpeed, rb.linearVelocity.y);

            if (direction > 0)
                WalkDirection = WalkableDirection.Right;
            else
                WalkDirection = WalkableDirection.Left;
        }
        else // Khi không có mục tiêu hoặc không đuổi, dừng di chuyển
        {
            rb.linearVelocity = new Vector2(Mathf.Lerp(rb.linearVelocity.x, 0, walkStopRate), rb.linearVelocity.y);
        }
    }

    private void FlipDirection()
    {
        if (WalkDirection == WalkableDirection.Left)
        {
            WalkDirection = WalkableDirection.Right;
        }
        else if (WalkDirection == WalkableDirection.Right)
        {
            WalkDirection = WalkableDirection.Left;
        }
    }

    public void OnHit(int damage, Vector2 knockback)
    {
        damageable.Health -= damage;

        if (damageable.Health <= 0)
        {
            Die();
        }
        else
        {
            isKnockedBack = true;
            rb.linearVelocity = knockback;
            StartCoroutine(ResetKnockback());
        }
    }


    private IEnumerator ResetKnockback()
    {
        yield return new WaitForSeconds(knockbackTime);
        isKnockedBack = false;
    }

    public void SpawnHealthPickup()
    {
        if (healthPickupPrefab != null)
        {
            Instantiate(healthPickupPrefab, transform.position + Vector3.up * 0.5f, Quaternion.identity);
        }
    }

    private void OnDrawGizmos()
    {
        Gizmos.color = Color.red;
        Gizmos.DrawWireSphere(transform.position, attackRange);
    }

    private void PerformRandomAttack()
    {
        if (target != null)
        {
            targetPosition = target.position; // Lưu lại vị trí mới nhất của Player
        }

        isAttacking = true; // Đánh dấu là đang tấn công
        int randomAttack;
        if (damageable.Health <= damageable.MaxHealth * 0.5f) // Nếu máu ≤ 50%
        {
            int[] lowHealthAttacks = { 1, 4, 5 };
            randomAttack = lowHealthAttacks[UnityEngine.Random.Range(0, lowHealthAttacks.Length)];
        }
        else
        {
            randomAttack = UnityEngine.Random.Range(1, 4); // Attack từ 1-4 khi máu > 50%
        }
        animator.SetInteger("attackType", randomAttack);
        animator.SetTrigger("attack");
    }

    public void OnAttackAnimationEnd()
    {
        isAttacking = false; // Animation kết thúc
        AttackCooldown = UnityEngine.Random.Range(2.0f, 3.0f); // Đặt cooldown
    }

    public void Die()
    {
        rb.linearVelocity = Vector2.zero;
        animator.SetTrigger("dieTrigger");
        this.enabled = false;
    }
}