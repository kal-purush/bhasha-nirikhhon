using System.Collections;
using UnityEngine;

[RequireComponent(typeof(Rigidbody2D), typeof(TouchingDirections), typeof(Damageable))]
public class GoblinCave : MonoBehaviour
{
    public float walkAcceleration = 3f;
    public float maxSpeed = 3f;
    public float chaseSpeed = 4f;
    public float attackRange = 1.5f;
    public DetectionZone attackZone;
    public GameObject healthPickupPrefab;
    public Transform target;
    public bool isKnockedBack = false;
    public float knockbackTime = 0.2f;
    public bool isChasing = false;

    Rigidbody2D rb;
    TouchingDirections touchingDirections;
    protected Animator animator;
    Damageable damageable;

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
        get { return animator.GetBool(AnimationStrings.canMove); }
    }

    public float AttackCooldown
    {
        get { return animator.GetFloat(AnimationStrings.attackCooldown); }
        private set { animator.SetFloat(AnimationStrings.attackCooldown, Mathf.Max(value, 0)); }
    }

    void Awake()
    {
        rb = GetComponent<Rigidbody2D>();
        touchingDirections = GetComponent<TouchingDirections>();
        animator = GetComponent<Animator>();
        damageable = GetComponent<Damageable>();
        WalkDirection = WalkableDirection.Left;

        // Ignore collision giữa Enemy và Player
        Collider2D enemyCollider = GetComponent<Collider2D>();
        Collider2D playerCollider = GameObject.FindGameObjectWithTag("Player")?.GetComponent<Collider2D>();
        if (playerCollider != null)
        {
            Physics2D.IgnoreCollision(enemyCollider, playerCollider);
        }
    }

    protected virtual void Update()
    {
        HasTarget = attackZone.detectedColliders.Count > 0;
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
                if (AttackCooldown <= 0)
                {
                    PerformRandomAttack();
                }
            }
        }
        else
        {
            target = null;
            isChasing = false;
            // Không đi tuần, chỉ đứng yên
        }
    }

    protected virtual void FixedUpdate()
    {
        if (isKnockedBack)
        {
            return;
        }

        if (target != null && isChasing && CanMove) // Chỉ di chuyển khi đuổi Player
        {
            float direction = Mathf.Sign(target.position.x - transform.position.x);
            rb.linearVelocity = new Vector2(direction * chaseSpeed, rb.linearVelocity.y);

            if (direction > 0)
                WalkDirection = WalkableDirection.Right;
            else
                WalkDirection = WalkableDirection.Left;
        }
        else
        {
            // Đứng yên khi không đuổi Player
            rb.linearVelocity = new Vector2(0, rb.linearVelocity.y);
        }
    }

    private void PerformRandomAttack()
    {
        int randomAttack = UnityEngine.Random.Range(1, 4); // Sinh số từ 1 đến 3
        animator.SetInteger("attackType", randomAttack);
        animator.SetTrigger("attack");
        AttackCooldown = UnityEngine.Random.Range(2.5f, 4f);
    }

    public void OnHit(int damage, Vector2 knockback)
    {
        isKnockedBack = true;
        rb.linearVelocity = knockback;
        StartCoroutine(ResetKnockback());
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

    public void Die()
    {
        rb.linearVelocity = Vector2.zero;
        GetComponent<Collider2D>().enabled = false;
        StartCoroutine(FallDownAfterDeath());
        this.enabled = false;
    }

    private IEnumerator FallDownAfterDeath()
    {
        float elapsedTime = 0f;
        float duration = 0.5f;
        Vector2 startPos = transform.position;
        Vector2 endPos = new Vector2(transform.position.x, transform.position.y - 0.5f);

        while (elapsedTime < duration)
        {
            transform.position = Vector2.Lerp(startPos, endPos, elapsedTime / duration);
            elapsedTime += Time.deltaTime;
            yield return null;
        }
        transform.position = endPos;
        rb.bodyType = RigidbodyType2D.Static;
    }

    private void OnDrawGizmos()
    {
        Gizmos.color = Color.red;
        Gizmos.DrawWireSphere(transform.position, attackRange);
    }
}